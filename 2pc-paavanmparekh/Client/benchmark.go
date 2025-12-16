package main

import (
	configurations "2pc-paavanmparekh/Configurations"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"time"
)

type BenchmarkConfig struct {
	TotalTransactions  int     `json:"totalTxns"`
	TransactionsPerSet int     `json:"txnsPerSet"`
	ReadOnlyPct        float64 `json:"readOnlyPct"`
	CrossShardPct      float64 `json:"crossShardPct"`
	Skew               float64 `json:"skew"`
	Runs               int     `json:"runs"`
	Seed               int64   `json:"seed"`
	AmountMin          int     `json:"amountMin"`
	AmountMax          int     `json:"amountMax"`
}

type clusterSampler struct {
	start int
	count int
	zipf  *rand.Zipf
}

type benchmarkTotals struct {
	txnCount     int
	totalLatency time.Duration
	totalRuntime time.Duration
}

func runBenchmark(path string) error {
	cfg, err := loadBenchmarkConfig(path)
	if err != nil {
		return err
	}
	runtime.GOMAXPROCS(runtime.NumCPU())

	ensureBenchmarkLogger()
	enableBenchmarkMode()
	configureNodesBenchmarkMode(true)
	defer func() {
		configureNodesBenchmarkMode(false)
		disableBenchmarkMode()
	}()

	benchmarkPrintf("Benchmarking with config: %+v\n", cfg)
	totalResults := benchmarkTotals{}
	clientTimestamps := make(map[int]int)
	var timestampMu sync.Mutex

	for run := 1; run <= cfg.Runs; run++ {
		benchmarkPrintf("\n--- Benchmark Run %d/%d ---\n", run, cfg.Runs)
		flushSystem()
		resetClientTimestamps(clientTimestamps)
		applyInitialLiveness(allNodeIDs())

		rng := rand.New(rand.NewSource(cfg.Seed + int64(run)))
		samplers, clusterIDs := newClusterSamplers(rng, cfg.Skew)
		if len(clusterIDs) == 0 {
			return fmt.Errorf("no clusters available for benchmarking")
		}
		sets := generateBenchmarkSets(cfg, rng, samplers, clusterIDs)

		for idx, setItems := range sets {
			metrics := newPerformanceTracker()
			processBenchmarkSet(context.Background(), setItems, clientTimestamps, &timestampMu, metrics)
			count, duration, throughput, avgLatency := metrics.summary()
			benchmarkPrintf("Run %d Set %d: txns=%d throughput=%.2f txn/s avg latency=%s duration=%s\n",
				run, idx+1, count, throughput, avgLatency, duration)
			totalResults.consume(metrics)
			flushSystem()
		}
	}

	totalResults.report(cfg.Runs)
	return nil
}

func loadBenchmarkConfig(path string) (BenchmarkConfig, error) {
	var cfg BenchmarkConfig
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}
	if cfg.TotalTransactions <= 0 {
		cfg.TotalTransactions = 300
	}
	if cfg.TransactionsPerSet <= 0 || cfg.TransactionsPerSet > cfg.TotalTransactions {
		cfg.TransactionsPerSet = int(math.Min(float64(cfg.TotalTransactions), 50))
	}
	if cfg.Runs <= 0 {
		cfg.Runs = 1
	}
	cfg.ReadOnlyPct = clampFloat(cfg.ReadOnlyPct, 0, 1)
	cfg.CrossShardPct = clampFloat(cfg.CrossShardPct, 0, 1)
	cfg.Skew = clampFloat(cfg.Skew, 0, 1)
	if cfg.AmountMin <= 0 {
		cfg.AmountMin = 1
	}
	if cfg.AmountMax < cfg.AmountMin {
		cfg.AmountMax = cfg.AmountMin
	}
	if cfg.Seed == 0 {
		cfg.Seed = time.Now().UnixNano()
	}
	return cfg, nil
}

func clampFloat(val, min, max float64) float64 {
	if val < min {
		return min
	}
	if val > max {
		return max
	}
	return val
}

func resetClientTimestamps(m map[int]int) {
	for k := range m {
		delete(m, k)
	}
}

func newClusterSamplers(rng *rand.Rand, skew float64) (map[int]*clusterSampler, []int) {
	clusterCount := configurations.ClusterCount()
	samplers := make(map[int]*clusterSampler, clusterCount)
	if clusterCount <= 0 {
		return samplers, nil
	}
	var zipfParam float64
	if skew > 0 {
		zipfParam = 1.0001 + skew*1.5
	}
	clusterIDs := make([]int, 0, clusterCount)
	for clusterID := 1; clusterID <= clusterCount; clusterID++ {
		start, end := configurations.GetClusterKeyRange(clusterID)
		if start <= 0 || end < start {
			continue
		}
		count := end - start + 1
		cs := &clusterSampler{start: start, count: count}
		if skew > 0 && count > 1 {
			cs.zipf = rand.NewZipf(rng, zipfParam, 1, uint64(count-1))
		}
		samplers[clusterID] = cs
		clusterIDs = append(clusterIDs, clusterID)
	}
	return samplers, clusterIDs
}

func (cs *clusterSampler) sample(rng *rand.Rand) int {
	if cs.count <= 1 {
		return cs.start
	}
	if cs.zipf != nil {
		idx := int(cs.zipf.Uint64())
		if idx >= cs.count {
			idx = cs.count - 1
		}
		return cs.start + idx
	}
	return cs.start + rng.Intn(cs.count)
}

func generateBenchmarkSets(cfg BenchmarkConfig, rng *rand.Rand, samplers map[int]*clusterSampler, clusterIDs []int) [][]OrderedItem {
	setsNeeded := int(math.Ceil(float64(cfg.TotalTransactions) / float64(cfg.TransactionsPerSet)))
	results := make([][]OrderedItem, 0, setsNeeded)
	for setIdx := 0; setIdx < setsNeeded; setIdx++ {
		remaining := cfg.TotalTransactions - setIdx*cfg.TransactionsPerSet
		count := cfg.TransactionsPerSet
		if remaining < count {
			count = remaining
		}
		items := make([]OrderedItem, 0, count)
		for i := 0; i < count; i++ {
			txn := synthesizeTransaction(cfg, rng, samplers, clusterIDs)
			items = append(items, OrderedItem{Kind: "txn", Txn: txn})
		}
		results = append(results, items)
	}
	return results
}

func synthesizeTransaction(cfg BenchmarkConfig, rng *rand.Rand, samplers map[int]*clusterSampler, clusterIDs []int) configurations.Transaction {
	if len(clusterIDs) == 0 {
		return configurations.Transaction{}
	}

	pickSampler := func(clusterID int) *clusterSampler {
		if sampler, ok := samplers[clusterID]; ok && sampler != nil {
			return sampler
		}
		for _, id := range clusterIDs {
			if sampler := samplers[id]; sampler != nil {
				return sampler
			}
		}
		return nil
	}

	randomCluster := func() int {
		return clusterIDs[rng.Intn(len(clusterIDs))]
	}

	if rng.Float64() < cfg.ReadOnlyPct {
		cluster := randomCluster()
		sampler := pickSampler(cluster)
		if sampler == nil {
			return configurations.Transaction{}
		}
		key := sampler.sample(rng)
		return configurations.Transaction{Sender: key, Receiver: key, ReadOnly: true}
	}

	senderCluster := randomCluster()
	receiverCluster := senderCluster
	if rng.Float64() < cfg.CrossShardPct && len(clusterIDs) > 1 {
		receiverCluster = chooseDifferentCluster(rng, senderCluster, clusterIDs)
	}

	senderSampler := pickSampler(senderCluster)
	receiverSampler := pickSampler(receiverCluster)
	if senderSampler == nil || receiverSampler == nil {
		return configurations.Transaction{}
	}

	senderKey := senderSampler.sample(rng)
	receiverKey := receiverSampler.sample(rng)
	if receiverCluster == senderCluster && receiverSampler.count > 1 {
		receiverKey = ensureDifferentKey(rng, receiverSampler, senderKey)
	}

	return configurations.Transaction{
		Sender:   senderKey,
		Receiver: receiverKey,
		Amount:   randomAmount(cfg, rng),
	}
}

func chooseDifferentCluster(rng *rand.Rand, exclude int, clusterIDs []int) int {
	if len(clusterIDs) == 0 {
		return exclude
	}
	candidates := make([]int, 0, len(clusterIDs)-1)
	for _, id := range clusterIDs {
		if id != exclude {
			candidates = append(candidates, id)
		}
	}
	if len(candidates) == 0 {
		return exclude
	}
	return candidates[rng.Intn(len(candidates))]
}

func clampKey(key int, sampler *clusterSampler) int {
	min := sampler.start
	max := sampler.start + sampler.count - 1
	if key < min {
		return min
	}
	if key > max {
		return max
	}
	return key
}

func ensureDifferentKey(rng *rand.Rand, sampler *clusterSampler, senderKey int) int {
	if sampler.count <= 1 {
		return senderKey
	}
	relative := senderKey - sampler.start
	if relative < 0 || relative >= sampler.count {
		relative = rng.Intn(sampler.count)
	}
	delta := rng.Intn(sampler.count-1) + 1
	newRelative := (relative + delta) % sampler.count
	return sampler.start + newRelative
}

func randomAmount(cfg BenchmarkConfig, rng *rand.Rand) int {
	if cfg.AmountMax == cfg.AmountMin {
		return cfg.AmountMin
	}
	return cfg.AmountMin + rng.Intn(cfg.AmountMax-cfg.AmountMin+1)
}

func (bt *benchmarkTotals) consume(pt *performanceTracker) {
	count, latency, duration := pt.snapshotTotals()
	bt.txnCount += count
	bt.totalLatency += latency
	bt.totalRuntime += duration
}

func (bt *benchmarkTotals) report(runs int) {
	if bt.txnCount == 0 {
		if !benchmarkingEnabled() {
			fmt.Println("Benchmark completed: no transactions executed.")
		}
		return
	}
	avgLatency := time.Duration(int64(bt.totalLatency) / int64(bt.txnCount))
	var throughput float64
	if bt.totalRuntime > 0 {
		throughput = float64(bt.txnCount) / bt.totalRuntime.Seconds()
	}
	if benchmarkingEnabled() {
		fmt.Printf("\nBenchmark summary across %d run(s):\n", runs)
		fmt.Printf("Total txns=%d, overall throughput=%.2f txn/s, avg latency=%s, wall-clock duration=%s\n",
			bt.txnCount, throughput, avgLatency, bt.totalRuntime)
		return
	}
	fmt.Printf("\nBenchmark summary across %d run(s):\n", runs)
	fmt.Printf("Total txns=%d, overall throughput=%.2f txn/s, avg latency=%s, wall-clock duration=%s\n",
		bt.txnCount, throughput, avgLatency, bt.totalRuntime)
}

func processBenchmarkSet(ctx context.Context, items []OrderedItem, clientTimestamps map[int]int, timestampMu *sync.Mutex, perf *performanceTracker) {
	txns := make([]configurations.Transaction, 0, len(items))
	for _, item := range items {
		if item.Kind == "txn" {
			txns = append(txns, item.Txn)
		}
	}
	if len(txns) == 0 {
		return
	}
	workerCount := benchmarkWorkerCount(len(txns))
	if workerCount <= 1 {
		runTxnBatch(ctx, txns, clientTimestamps, timestampMu, perf, nil)
		return
	}

	chunkSize := benchmarkChunkSize(len(txns), workerCount)
	jobCh := make(chan []configurations.Transaction, workerCount*2)
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := range jobCh {
				if ctx.Err() != nil {
					return
				}
				runTxnBatch(ctx, chunk, clientTimestamps, timestampMu, perf, nil)
			}
		}()
	}

	for start := 0; start < len(txns); start += chunkSize {
		if ctx.Err() != nil {
			break
		}
		end := start + chunkSize
		if end > len(txns) {
			end = len(txns)
		}
		chunk := make([]configurations.Transaction, end-start)
		copy(chunk, txns[start:end])
		jobCh <- chunk
	}
	close(jobCh)
	wg.Wait()
}

func benchmarkWorkerCount(total int) int {
	if total <= 0 {
		return 1
	}
	// Moderate parallelism to avoid overwhelming node DBs.
	workers := total / 4
	if workers < 1 {
		workers = 1
	}
	if workers > 24 {
		workers = 24
	}
	return workers
}

func benchmarkChunkSize(total, workers int) int {
	if total <= 0 {
		return 0
	}
	if workers < 1 {
		workers = 1
	}
	targetChunks := workers * 4
	if targetChunks < 1 {
		targetChunks = 1
	}
	size := int(math.Ceil(float64(total) / float64(targetChunks)))
	if size < 25 {
		size = 25
	}
	if size > 200 {
		size = 200
	}
	if size > total {
		size = total
	}
	return size
}
