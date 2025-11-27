package main

import (
	configurations "2pc-paavanmparekh/Configurations"
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	inputCSVPath = "../Input/CSE535-F25-Project-3-Testcases.csv"
	flatCSVPath  = "../Input/CSE535-F25-Project-3-Testcases_flat.csv"
)

type OrderedItem struct {
	Kind string
	Txn  configurations.Transaction
	Node int
}

type reshardMove struct {
	Key  int
	From int
	To   int
}

type performanceTracker struct {
	mu            sync.Mutex
	txnCount      int
	totalLatency  time.Duration
	earliestStart time.Time
	latestEnd     time.Time
}

func newPerformanceTracker() *performanceTracker {
	return &performanceTracker{}
}

var (
	lastSetMu           sync.RWMutex
	lastSetTransactions []configurations.Transaction
	reshardMu           sync.Mutex
)

func (p *performanceTracker) record(start, end time.Time) {
	if end.Before(start) {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.earliestStart.IsZero() || start.Before(p.earliestStart) {
		p.earliestStart = start
	}
	if end.After(p.latestEnd) {
		p.latestEnd = end
	}
	p.txnCount++
	p.totalLatency += end.Sub(start)
}

func (p *performanceTracker) summary() (count int, duration time.Duration, throughput float64, avgLatency time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	count = p.txnCount
	if !p.earliestStart.IsZero() && !p.latestEnd.IsZero() && p.latestEnd.After(p.earliestStart) {
		duration = p.latestEnd.Sub(p.earliestStart)
	}
	if count > 0 {
		if duration > 0 {
			throughput = float64(count) / duration.Seconds()
		}
		avgLatency = time.Duration(int64(p.totalLatency) / int64(count))
	}
	return
}

func startInputReader() <-chan string {
	ch := make(chan string)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			ch <- scanner.Text()
		}
		close(ch)
	}()
	return ch
}

func triggerPrintReshardCommand() {
	lastSetMu.RLock()
	snapshot := append([]configurations.Transaction(nil), lastSetTransactions...)
	lastSetMu.RUnlock()
	if len(snapshot) == 0 {
		fmt.Println("PrintReshard: no completed set available yet.")
		return
	}
	fmt.Println("PrintReshard: analyzing last completed set...")
	handlePrintReshard(snapshot)
}

func main() {
	if err := flattenCSV(inputCSVPath, flatCSVPath); err != nil {
		log.Fatalf("failed to flatten csv: %v", err)
	}

	setItems, setLiveNodes, err := loadOrderedItems(flatCSVPath)
	if err != nil {
		log.Fatalf("failed to load ordered items: %v", err)
	}

	clientTimestamps := make(map[int]int)
	var timestampMu sync.Mutex

	setNumbers := collectAndSortSetIDs(setItems)
	inputCh := startInputReader()
	fmt.Println("Controls: Press Enter to run a set, type 'next' while it runs to skip the remainder, and type 'printreshard' between sets to rebalance the last completed set.")

	for _, setID := range setNumbers {
		currentSetTxns := make([]configurations.Transaction, 0, len(setItems[setID]))
		fmt.Printf("\n===== Set %s =====\n", setID)
		fmt.Print("Press Enter to start this set (or type 'next' to skip): ")

		startSet := true
	startLoop:
		for {
			line, ok := <-inputCh
			if !ok {
				startSet = false
				break
			}
			cmd := strings.TrimSpace(strings.ToLower(line))
			if cmd == "" {
				break startLoop
			}
			if cmd == "next" {
				startSet = false
				break startLoop
			}
			if cmd == "printreshard" {
				triggerPrintReshardCommand()
				fmt.Print("Press Enter to start this set (or type 'next' to skip): ")
				continue
			}
			fmt.Print("Press Enter to start this set (or type 'next' to skip): ")
		}

		if !startSet {
			fmt.Printf("Skipping set %s\n", setID)
			fmt.Printf("===== Completed set %s =====\n", setID)
			continue
		}

		fmt.Println("Flushing system state...")
		flushSystem()
		for clientID := range clientTimestamps {
			delete(clientTimestamps, clientID)
		}

		liveNodes := setLiveNodes[setID]
		if len(liveNodes) == 0 {
			liveNodes = allNodeIDs()
		}
		applyInitialLiveness(liveNodes)

		metrics := newPerformanceTracker()
		ctx, cancel := context.WithCancel(context.Background())
		doneCh := make(chan struct{})
		go func(txnLog *[]configurations.Transaction) {
			processSet(ctx, setItems[setID], clientTimestamps, &timestampMu, metrics, txnLog)
			close(doneCh)
		}(&currentSetTxns)

	setActive:
		for {
			select {
			case <-doneCh:
				break setActive
			case line, ok := <-inputCh:
				if !ok {
					cancel()
					<-doneCh
					break setActive
				}
				cmd := strings.TrimSpace(strings.ToLower(line))
				if cmd == "next" {
					fmt.Println("Skipping remaining transactions for this set...")
					cancel()
					<-doneCh
					break setActive
				} else if cmd == "printreshard" {
					fmt.Println("Cannot run PrintReshard while a set is executing. Wait until it completes.")
				} else if cmd != "" {
					fmt.Println("Unknown command while set running. Type 'next' to skip.")
				}
			}
		}
		cancel()
		reportPerformance(setID, metrics)
		if startSet {
			lastSetMu.Lock()
			lastSetTransactions = append([]configurations.Transaction(nil), currentSetTxns...)
			lastSetMu.Unlock()
		}
		fmt.Printf("===== Completed set %s =====\n", setID)

	}

	fmt.Println("All sets processed")
}

func flattenCSV(inPath, outPath string) error {
	inFile, err := os.Open(inPath)
	if err != nil {
		return err
	}
	defer inFile.Close()

	reader := csv.NewReader(inFile)
	reader.FieldsPerRecord = -1
	rows, err := reader.ReadAll()
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return fmt.Errorf("input csv empty")
	}

	if err := os.MkdirAll("Input", 0755); err != nil {
		return err
	}

	outFile, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	writer := csv.NewWriter(outFile)
	defer writer.Flush()

	if err := writer.Write(rows[0]); err != nil {
		return err
	}

	currentSet := ""
	currentLive := ""
	for _, row := range rows[1:] {
		for len(row) < 3 {
			row = append(row, "")
		}
		if strings.TrimSpace(row[0]) != "" {
			currentSet = strings.TrimSpace(row[0])
		}
		if strings.TrimSpace(row[2]) != "" {
			currentLive = strings.TrimSpace(row[2])
		}
		if currentSet == "" {
			continue
		}
		if err := writer.Write([]string{currentSet, row[1], currentLive}); err != nil {
			return err
		}
	}
	return nil
}

func loadOrderedItems(path string) (map[string][]OrderedItem, map[string][]int, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	if err != nil {
		return nil, nil, err
	}
	if len(records) == 0 {
		return nil, nil, fmt.Errorf("no records in csv")
	}

	setItems := make(map[string][]OrderedItem)
	setLiveNodes := make(map[string][]int)

	for _, rec := range records[1:] {
		for len(rec) < 3 {
			rec = append(rec, "")
		}
		setID := strings.TrimSpace(rec[0])
		entry := strings.TrimSpace(rec[1])
		live := strings.TrimSpace(rec[2])
		if setID == "" {
			continue
		}
		if live != "" {
			setLiveNodes[setID] = parseLiveNodes(live)
		}
		if entry == "" {
			continue
		}
		if kind, nodeID, ok := parseNodeCommand(entry); ok {
			setItems[setID] = append(setItems[setID], OrderedItem{Kind: kind, Node: nodeID})
			continue
		}
		txn, err := parseTransaction(entry)
		if err != nil {
			log.Printf("Skipping invalid transaction %q: %v", entry, err)
			continue
		}
		setItems[setID] = append(setItems[setID], OrderedItem{Kind: "txn", Txn: txn})
	}

	return setItems, setLiveNodes, nil
}

func parseTransaction(raw string) (configurations.Transaction, error) {
	raw = strings.TrimSpace(raw)
	raw = strings.Trim(raw, "() ")
	if raw == "" {
		return configurations.Transaction{}, fmt.Errorf("empty transaction")
	}

	parts := strings.Split(raw, ",")
	if len(parts) == 1 {
		sender, err := parseAccountToken(parts[0])
		if err != nil {
			return configurations.Transaction{}, err
		}
		return configurations.Transaction{Sender: sender, Receiver: sender, ReadOnly: true}, nil
	}

	if len(parts) < 3 {
		return configurations.Transaction{}, fmt.Errorf("invalid transaction format: %s", raw)
	}
	sender, err := parseAccountToken(parts[0])
	if err != nil {
		return configurations.Transaction{}, err
	}
	receiver, err := parseAccountToken(parts[1])
	if err != nil {
		return configurations.Transaction{}, err
	}
	amount, err := strconv.Atoi(strings.TrimSpace(parts[2]))
	if err != nil {
		return configurations.Transaction{}, err
	}
	return configurations.Transaction{Sender: sender, Receiver: receiver, Amount: amount}, nil
}

func parseAccountToken(token string) (int, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return 0, fmt.Errorf("empty account token")
	}
	if len(token) == 1 && token[0] >= 'A' && token[0] <= 'Z' {
		return int(token[0]-'A') + 1, nil
	}
	return strconv.Atoi(token)
}

func parseNodeCommand(entry string) (string, int, bool) {
	entry = strings.TrimSpace(entry)
	if len(entry) < 4 {
		return "", 0, false
	}
	prefix := entry[:2]
	if prefix != "F(" && prefix != "R(" {
		return "", 0, false
	}
	cmd := "fail"
	if prefix == "R(" {
		cmd = "recover"
	}
	nodeStr := strings.TrimSuffix(strings.TrimPrefix(entry, prefix), ")")
	nodeStr = strings.TrimSpace(nodeStr)
	nodeStr = strings.TrimPrefix(nodeStr, "n")
	nodeID, err := strconv.Atoi(nodeStr)
	if err != nil {
		return "", 0, false
	}
	return cmd, nodeID, true
}

func parseLiveNodes(raw string) []int {
	raw = strings.TrimSpace(raw)
	raw = strings.Trim(raw, "[]")
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	var nodes []int
	for _, part := range parts {
		part = strings.TrimSpace(part)
		part = strings.TrimPrefix(part, "n")
		if part == "" {
			continue
		}
		if nodeID, err := strconv.Atoi(part); err == nil {
			nodes = append(nodes, nodeID)
		}
	}
	return nodes
}

func collectAndSortSetIDs(items map[string][]OrderedItem) []string {
	setIDs := make([]string, 0, len(items))
	for id := range items {
		setIDs = append(setIDs, id)
	}
	sort.Slice(setIDs, func(i, j int) bool {
		li, err1 := strconv.Atoi(setIDs[i])
		lj, err2 := strconv.Atoi(setIDs[j])
		if err1 != nil || err2 != nil {
			return setIDs[i] < setIDs[j]
		}
		return li < lj
	})
	return setIDs
}

func processSet(ctx context.Context, items []OrderedItem, clientTimestamps map[int]int, timestampMu *sync.Mutex, perf *performanceTracker, recorded *[]configurations.Transaction) {
	var segment []configurations.Transaction
	for _, item := range items {
		if ctx.Err() != nil {
			return
		}
		switch item.Kind {
		case "txn":
			segment = append(segment, item.Txn)
		case "fail":
			processSegmentTransactions(ctx, segment, clientTimestamps, timestampMu, perf, recorded)
			if ctx.Err() != nil {
				return
			}
			segment = nil
			fmt.Printf("Failing node n%d\n", item.Node)
			setNodeStatus(item.Node, false)
		case "recover":
			processSegmentTransactions(ctx, segment, clientTimestamps, timestampMu, perf, recorded)
			if ctx.Err() != nil {
				return
			}
			segment = nil
			fmt.Printf("Recovering node n%d\n", item.Node)
			setNodeStatus(item.Node, true)
		}
	}
	processSegmentTransactions(ctx, segment, clientTimestamps, timestampMu, perf, recorded)
}

var clusterLeaders = map[int]int{
	1: 1,
	2: 4,
	3: 7,
}
var leaderMutex sync.RWMutex

func processSegmentTransactions(ctx context.Context, transactions []configurations.Transaction, clientTimestamps map[int]int, timestampMu *sync.Mutex, perf *performanceTracker, recorded *[]configurations.Transaction) {
	if len(transactions) == 0 || ctx.Err() != nil {
		return
	}

	var wg sync.WaitGroup
	for _, txn := range transactions {
		if ctx.Err() != nil {
			break
		}

		timestampMu.Lock()
		if clientTimestamps[txn.Sender] == 0 {
			clientTimestamps[txn.Sender] = 1
		}
		txn.Timestamp = clientTimestamps[txn.Sender]
		clientTimestamps[txn.Sender]++
		timestampMu.Unlock()

		if recorded != nil {
			*recorded = append(*recorded, txn)
		}

		txnCopy := txn
		wg.Add(1)
		go func(t configurations.Transaction) {
			defer wg.Done()
			if ctx.Err() != nil {
				return
			}
			start := time.Now()
			if err := sendTransaction(ctx, t); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				log.Printf("Transaction (%d -> %d) failed: %v", t.Sender, t.Receiver, err)
				return
			}
			if perf != nil {
				perf.record(start, time.Now())
			}
			if err := handleCrossShardCredit(ctx, t, clientTimestamps, timestampMu, perf); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				log.Printf("Cross-shard credit for txn (%d -> %d) failed: %v", t.Sender, t.Receiver, err)
			}
		}(txnCopy)
	}
	wg.Wait()
}

func reportPerformance(setID string, tracker *performanceTracker) {
	if tracker == nil {
		return
	}
	count, duration, throughput, avgLatency := tracker.summary()
	if count == 0 {
		fmt.Printf("Performance for set %s: no transactions executed\n", setID)
		return
	}
	fmt.Printf("Performance for set %s: txns=%d, throughput=%.2f txn/s, avg latency=%s, duration=%s\n",
		setID, count, throughput, avgLatency, duration)
}

func handlePrintReshard(txns []configurations.Transaction) {
	reshardMu.Lock()
	defer reshardMu.Unlock()
	moves, err := executeReshard(txns)
	if err != nil {
		fmt.Printf("PrintReshard failed: %v\n", err)
		return
	}
	if len(moves) == 0 {
		fmt.Println("PrintReshard: no data movement required for the last set.")
		return
	}
	fmt.Println("PrintReshard results:")
	for _, move := range moves {
		fmt.Printf("(%d, c%d, c%d)\n", move.Key, move.From, move.To)
	}
}

func handleCrossShardCredit(ctx context.Context, txn configurations.Transaction, clientTimestamps map[int]int, timestampMu *sync.Mutex, perf *performanceTracker) error {
	if txn.ReadOnly || txn.CreditOnly {
		return nil
	}
	senderCluster := configurations.ResolveClusterIDForKey(txn.Sender)
	receiverCluster := configurations.ResolveClusterIDForKey(txn.Receiver)
	if senderCluster == 0 || receiverCluster == 0 || senderCluster == receiverCluster {
		return nil
	}

	creditTxn := configurations.Transaction{
		Sender:     txn.Receiver,
		Receiver:   txn.Receiver,
		Amount:     txn.Amount,
		CreditOnly: true,
	}

	timestampMu.Lock()
	if clientTimestamps[creditTxn.Sender] == 0 {
		clientTimestamps[creditTxn.Sender] = 1
	}
	creditTxn.Timestamp = clientTimestamps[creditTxn.Sender]
	clientTimestamps[creditTxn.Sender]++
	timestampMu.Unlock()

	start := time.Now()
	if err := sendTransaction(ctx, creditTxn); err != nil {
		return err
	}
	if perf != nil {
		perf.record(start, time.Now())
	}
	return nil
}

func executeReshard(txns []configurations.Transaction) ([]reshardMove, error) {
	if len(txns) == 0 {
		return nil, fmt.Errorf("no transactions available for resharding")
	}
	assignments := buildReshardAssignments(txns)
	if len(assignments) == 0 {
		return nil, nil
	}
	return applyReshardPlan(assignments)
}

func buildReshardAssignments(txns []configurations.Transaction) map[int]int {
	adj := make(map[int]map[int]int)
	nodes := make(map[int]struct{})
	for _, txn := range txns {
		if txn.Sender != 0 {
			nodes[txn.Sender] = struct{}{}
		}
		if txn.Receiver != 0 {
			nodes[txn.Receiver] = struct{}{}
		}
		if txn.Sender == txn.Receiver {
			continue
		}
		if txn.Sender != 0 && txn.Receiver != 0 {
			if adj[txn.Sender] == nil {
				adj[txn.Sender] = make(map[int]int)
			}
			if adj[txn.Receiver] == nil {
				adj[txn.Receiver] = make(map[int]int)
			}
			adj[txn.Sender][txn.Receiver]++
			adj[txn.Receiver][txn.Sender]++
		}
	}

	keys := make([]int, 0, len(nodes))
	for key := range nodes {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		di := 0
		for _, weight := range adj[keys[i]] {
			di += weight
		}
		dj := 0
		for _, weight := range adj[keys[j]] {
			dj += weight
		}
		if di == dj {
			return keys[i] < keys[j]
		}
		return di > dj
	})

	assignments := make(map[int]int, len(keys))
	if len(keys) == 0 {
		return assignments
	}

	capacity := make([]int, 3)
	base := len(keys) / 3
	rem := len(keys) % 3
	for i := 0; i < 3; i++ {
		capacity[i] = base
		if i < rem {
			capacity[i]++
		}
		if capacity[i] == 0 && len(keys) > 0 {
			capacity[i] = 1
		}
	}
	sizes := make([]int, 3)
	const bigPenalty = 1000

	for _, key := range keys {
		bestCluster := 1
		bestScore := int(^uint(0) >> 1)
		for clusterIdx := 0; clusterIdx < 3; clusterIdx++ {
			score := 0
			if capacity[clusterIdx] > 0 && sizes[clusterIdx] >= capacity[clusterIdx] {
				score += (sizes[clusterIdx] - capacity[clusterIdx] + 1) * bigPenalty
			}
			if defaultCluster := configurations.DefaultClusterForKey(key); defaultCluster == clusterIdx+1 {
				score--
			}
			if neighbors, ok := adj[key]; ok {
				for neighbor, weight := range neighbors {
					if assignedCluster, exists := assignments[neighbor]; exists {
						if assignedCluster == clusterIdx+1 {
							score -= weight
						} else {
							score += weight
						}
					}
				}
			}
			if score < bestScore {
				bestScore = score
				bestCluster = clusterIdx + 1
			}
		}
		assignments[key] = bestCluster
		sizes[bestCluster-1]++
	}

	return assignments
}

func applyReshardPlan(assignments map[int]int) ([]reshardMove, error) {
	currentOverrides := configurations.GetShardOverrides()
	newOverrides := make(map[int]int, len(currentOverrides))
	for k, v := range currentOverrides {
		newOverrides[k] = v
	}
	var moves []reshardMove
	for key, targetCluster := range assignments {
		if targetCluster == 0 {
			continue
		}
		currentCluster := configurations.ResolveClusterIDForKey(key)
		if currentCluster == 0 || currentCluster == targetCluster {
			if targetCluster != configurations.DefaultClusterForKey(key) {
				newOverrides[key] = targetCluster
			} else {
				delete(newOverrides, key)
			}
			continue
		}
		balance, err := fetchBalanceFromCluster(key, currentCluster)
		if err != nil {
			return nil, fmt.Errorf("failed to read key %d from c%d: %w", key, currentCluster, err)
		}
		if err := applyBalanceToCluster(key, balance, targetCluster); err != nil {
			return nil, fmt.Errorf("failed to apply key %d to c%d: %w", key, targetCluster, err)
		}
		if err := deleteKeyFromCluster(key, currentCluster); err != nil {
			return nil, fmt.Errorf("failed to delete key %d from c%d: %w", key, currentCluster, err)
		}
		moves = append(moves, reshardMove{Key: key, From: currentCluster, To: targetCluster})
		if targetCluster != configurations.DefaultClusterForKey(key) {
			newOverrides[key] = targetCluster
		} else {
			delete(newOverrides, key)
		}
	}
	if err := configurations.SetShardOverrides(newOverrides); err != nil {
		return nil, err
	}
	return moves, nil
}

func fetchBalanceFromCluster(key, clusterID int) (int, error) {
	nodeIDs := configurations.GetClusterNodeIDs(clusterID)
	var lastErr error
	for _, nodeID := range nodeIDs {
		var balance int
		if err := callNodeRPC(nodeID, "Node.GetBalance", key, &balance); err != nil {
			lastErr = err
			continue
		}
		return balance, nil
	}
	if lastErr != nil {
		return 0, lastErr
	}
	return 0, fmt.Errorf("no nodes responded in cluster %d", clusterID)
}

func applyBalanceToCluster(key, balance, clusterID int) error {
	nodeIDs := configurations.GetClusterNodeIDs(clusterID)
	update := configurations.BalanceUpdate{Key: key, Balance: balance}
	for _, nodeID := range nodeIDs {
		var ack bool
		if err := callNodeRPC(nodeID, "Node.SetBalance", update, &ack); err != nil {
			return err
		}
	}
	return nil
}

func deleteKeyFromCluster(key, clusterID int) error {
	nodeIDs := configurations.GetClusterNodeIDs(clusterID)
	for _, nodeID := range nodeIDs {
		var ack bool
		if err := callNodeRPC(nodeID, "Node.DeleteBalance", key, &ack); err != nil {
			return err
		}
	}
	return nil
}

func sendTransaction(ctx context.Context, txn configurations.Transaction) error {
	cluster := configurations.GetClusterForKey(txn.Sender)
	if cluster == nil {
		return fmt.Errorf("no cluster found for sender %d", txn.Sender)
	}

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		leaderMutex.RLock()
		leaderID := clusterLeaders[cluster.Id]
		leaderMutex.RUnlock()

		resp, err := callClusterNode(txn, leaderID)
		if err != nil {
			resp, err = broadcastToCluster(txn, cluster.Id, leaderID)
		} else if resp != nil && resp.Msg == "Node Not Live" {
			resp, err = broadcastToCluster(txn, cluster.Id, leaderID)
		}

		if err == nil && resp != nil {
			if resp.B.NodeID != 0 {
				leaderMutex.Lock()
				clusterLeaders[cluster.Id] = resp.B.NodeID
				leaderMutex.Unlock()
			}

			if resp.Result {
				if txn.CreditOnly {
					return nil
				}
				if txn.ReadOnly {
					fmt.Printf("Txn READ(%d, ts %d) result: %s (ballot=%d node=%d ts=%d)\n",
						txn.Sender, txn.Timestamp, resp.Msg, resp.B.B, resp.B.NodeID, resp.Timestamp)
				} else {
					fmt.Printf("Txn (%d -> %d, amt %d, ts %d) result: %s (ballot=%d node=%d ts=%d)\n",
						txn.Sender, txn.Receiver, txn.Amount, txn.Timestamp, resp.Msg, resp.B.B, resp.B.NodeID, resp.Timestamp)
				}
				return nil
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func broadcastToCluster(txn configurations.Transaction, clusterID, skipNode int) (*configurations.Reply, error) {
	nodeIDs := configurations.GetClusterNodeIDs(clusterID)
	var lastErr error
	for _, nodeID := range nodeIDs {
		if nodeID == skipNode {
			continue
		}
		resp, err := callClusterNode(txn, nodeID)
		if err != nil {
			lastErr = err
			continue
		}
		if resp.Msg == "Node Not Live" {
			lastErr = fmt.Errorf("node %d not live", nodeID)
			continue
		}
		return resp, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("no responsive nodes in cluster %d", clusterID)
}

func callClusterNode(txn configurations.Transaction, nodeID int) (*configurations.Reply, error) {
	var resp configurations.Reply
	if err := callNodeRPC(nodeID, "Node.ClientRequest", txn, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func callNodeRPC(nodeID int, method string, req interface{}, resp interface{}) error {
	port := configurations.GetNodePort(nodeID)
	if port == 0 {
		return fmt.Errorf("no port for node %d", nodeID)
	}
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return fmt.Errorf("failed to connect to node %d: %w", nodeID, err)
	}
	defer client.Close()
	if err := client.Call(method, req, resp); err != nil {
		return fmt.Errorf("%s RPC to node %d failed: %w", method, nodeID, err)
	}
	return nil
}

func applyInitialLiveness(liveNodes []int) {
	liveSet := make(map[int]bool)
	for _, node := range liveNodes {
		liveSet[node] = true
	}
	for nodeID := 1; nodeID <= 9; nodeID++ {
		setNodeStatus(nodeID, liveSet[nodeID])
	}
}

func setNodeStatus(nodeID int, live bool) {
	port := configurations.GetNodePort(nodeID)
	if port == 0 {
		log.Printf("No port for node %d", nodeID)
		return
	}
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		log.Printf("Failed to connect to node %d: %v", nodeID, err)
		return
	}
	defer client.Close()

	var reply bool
	if err := client.Call("Node.FailNode", live, &reply); err != nil {
		log.Printf("FailNode RPC to n%d failed: %v", nodeID, err)
	}
}

func flushSystem() {
	for nodeID := 1; nodeID <= 9; nodeID++ {
		port := configurations.GetNodePort(nodeID)
		if port == 0 {
			continue
		}
		client, err := rpc.DialHTTP("tcp", fmt.Sprintf("localhost:%d", port))
		if err != nil {
			continue
		}
		var reply bool
		client.Call("Node.FlushState", true, &reply)
		client.Close()
	}
}

func allNodeIDs() []int {
	nodes := make([]int, 0, 9)
	for i := 1; i <= 9; i++ {
		nodes = append(nodes, i)
	}
	return nodes
}
