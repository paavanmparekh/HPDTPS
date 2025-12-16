package configurations

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

const (
	keyspaceMin = 1
	keyspaceMax = 9000
)

type ClusterLayout struct {
	ClusterCount    int `json:"clusterCount"`
	NodesPerCluster int `json:"nodesPerCluster"`
}

var (
	clusterLayout     ClusterLayout
	clusterLayoutOnce sync.Once
)

func clusterConfigPath() string {
	if _, err := os.Stat("Configurations"); err == nil {
		return filepath.Join("Configurations", "cluster_config.json")
	}
	return filepath.Join("..", "Configurations", "cluster_config.json")
}

func loadClusterLayoutFromDisk() ClusterLayout {
	layout := ClusterLayout{ClusterCount: 3, NodesPerCluster: 3}
	path := clusterConfigPath()
	bytes, err := os.ReadFile(path)
	if err != nil || len(bytes) == 0 {
		return layout
	}
	var diskLayout ClusterLayout
	if err := json.Unmarshal(bytes, &diskLayout); err != nil {
		return layout
	}
	if diskLayout.ClusterCount > 0 {
		layout.ClusterCount = diskLayout.ClusterCount
	}
	if diskLayout.NodesPerCluster > 0 {
		layout.NodesPerCluster = diskLayout.NodesPerCluster
	}
	return layout
}

func getClusterLayout() ClusterLayout {
	clusterLayoutOnce.Do(func() {
		clusterLayout = loadClusterLayoutFromDisk()
		if clusterLayout.ClusterCount <= 0 {
			clusterLayout.ClusterCount = 3
		}
		if clusterLayout.NodesPerCluster <= 0 {
			clusterLayout.NodesPerCluster = 3
		}
	})
	return clusterLayout
}

func ClusterCount() int {
	return getClusterLayout().ClusterCount
}

func NodesPerCluster() int {
	return getClusterLayout().NodesPerCluster
}

func TotalNodeCount() int {
	layout := getClusterLayout()
	if layout.ClusterCount <= 0 || layout.NodesPerCluster <= 0 {
		return 0
	}
	return layout.ClusterCount * layout.NodesPerCluster
}

type TwoPCPhase string

const (
	PhaseNone    TwoPCPhase = ""
	PhasePrepare TwoPCPhase = "P"
	PhaseCommit  TwoPCPhase = "C"
	PhaseAbort   TwoPCPhase = "A"
)

type TwoPCRole string

const (
	RoleNone        TwoPCRole = ""
	RoleCoordinator TwoPCRole = "coordinator"
	RoleParticipant TwoPCRole = "participant"
)

type Cluster struct {
	Id     int
	Nodes  []*Node
	Leader int
}

type Node struct {
	Id        int
	ClusterId int
	Port      int
	Db        *sql.DB
	Bnum      BallotNumber
	AcceptLog []AcceptLog

	ClientLastReply   map[int]int
	TxnsProcessed     map[string]Reply
	T                 time.Duration
	Tp                time.Duration
	IsLive            bool
	SequenceNumber    int
	Timestamp         int
	LastExecuted      int
	PendingCommands   map[int]AcceptTxn
	TransactionStatus map[int]string
}

type Transaction struct {
	Sender     int
	Receiver   int
	Amount     int
	Timestamp  int
	ReadOnly   bool
	CreditOnly bool
}

type TransactionResponse struct {
	Success bool
	Message string
}

// Paxos/Multi-Paxos message and log types.
type Reply struct {
	B         BallotNumber
	Timestamp int
	Result    bool
	Msg       string
}

type TxnReply struct {
	Res bool
	Msg string
}

type BallotNumber struct {
	B      int
	NodeID int
}

type AcceptLog struct {
	AcceptNum          BallotNumber
	AcceptSeq          int
	AcceptVal          Transaction
	Status             string
	Phase              TwoPCPhase
	Role               TwoPCRole
	TxnID              string
	IsCrossShard       bool
	CoordinatorCluster int
	ParticipantCluster int
}

type NewViewInput struct {
	B         BallotNumber
	AcceptLog []AcceptLog
}

type Promise struct {
	AcceptedMsgs []AcceptLog
	Vote         int
}

type AcceptTxn struct {
	B                  BallotNumber
	Txn                Transaction
	SeqNo              int
	Acceptance         int
	Status             string
	Phase              TwoPCPhase
	Role               TwoPCRole
	TxnID              string
	IsCrossShard       bool
	CoordinatorCluster int
	ParticipantCluster int
}

type StateSnapshot struct {
	LastExecuted int
	AcceptLog    []AcceptLog
}

type BalanceUpdate struct {
	Key     int
	Balance int
}

func shardMappingPath() string {
	if _, err := os.Stat("Configurations"); err == nil {
		return filepath.Join("Configurations", "shard_mapping.json")
	}
	return filepath.Join("..", "Configurations", "shard_mapping.json")
}

var (
	shardOverrides      map[int]int
	shardOverridesMu    sync.RWMutex
	shardOverridesClock time.Time
)

func maybeReloadShardOverrides() {
	path := shardMappingPath()
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			shardOverridesMu.Lock()
			if shardOverrides == nil {
				shardOverrides = make(map[int]int)
			} else if len(shardOverrides) != 0 {
				for k := range shardOverrides {
					delete(shardOverrides, k)
				}
			}
			shardOverridesClock = time.Time{}
			shardOverridesMu.Unlock()
		}
		return
	}
	needReload := false
	shardOverridesMu.RLock()
	if shardOverrides == nil || info.ModTime().After(shardOverridesClock) {
		needReload = true
	}
	shardOverridesMu.RUnlock()
	if !needReload {
		return
	}
	bytes, err := os.ReadFile(path)
	if err != nil {
		return
	}
	raw := make(map[string]int)
	if len(bytes) != 0 {
		if err := json.Unmarshal(bytes, &raw); err != nil {
			return
		}
	}
	updated := make(map[int]int, len(raw))
	maxCluster := getClusterLayout().ClusterCount
	for keyStr, clusterID := range raw {
		keyInt, err := strconv.Atoi(keyStr)
		if err != nil {
			continue
		}
		if clusterID < 1 || (maxCluster > 0 && clusterID > maxCluster) {
			continue
		}
		updated[keyInt] = clusterID
	}
	shardOverridesMu.Lock()
	shardOverrides = updated
	shardOverridesClock = info.ModTime()
	shardOverridesMu.Unlock()
}

func GetClusterKeyRange(clusterId int) (int, int) {
	layout := getClusterLayout()
	if clusterId < 1 || clusterId > layout.ClusterCount {
		return 0, 0
	}
	totalKeys := keyspaceMax - keyspaceMin + 1
	if totalKeys <= 0 {
		return 0, 0
	}
	base := totalKeys / layout.ClusterCount
	remainder := totalKeys % layout.ClusterCount
	start := keyspaceMin + (clusterId-1)*base
	if remainder > 0 {
		extra := clusterId - 1
		if extra > remainder {
			extra = remainder
		}
		start += extra
	}
	size := base
	if remainder > 0 && clusterId <= remainder {
		size++
	}
	if size <= 0 {
		return start, start - 1
	}
	end := start + size - 1
	if end > keyspaceMax {
		end = keyspaceMax
	}
	return start, end
}

func DefaultClusterForKey(key int) int {
	layout := getClusterLayout()
	for clusterId := 1; clusterId <= layout.ClusterCount; clusterId++ {
		start, end := GetClusterKeyRange(clusterId)
		if end < start {
			continue
		}
		if key >= start && key <= end {
			return clusterId
		}
	}
	return 0
}

func ResolveClusterIDForKey(key int) int {
	maybeReloadShardOverrides()
	maxCluster := getClusterLayout().ClusterCount
	shardOverridesMu.RLock()
	if shardOverrides != nil {
		if cid, ok := shardOverrides[key]; ok && cid >= 1 && cid <= maxCluster {
			shardOverridesMu.RUnlock()
			return cid
		}
	}
	shardOverridesMu.RUnlock()
	return DefaultClusterForKey(key)
}

func GetShardOverrides() map[int]int {
	maybeReloadShardOverrides()
	shardOverridesMu.RLock()
	defer shardOverridesMu.RUnlock()
	result := make(map[int]int, len(shardOverrides))
	for k, v := range shardOverrides {
		result[k] = v
	}
	return result
}

func SetShardOverrides(overrides map[int]int) error {
	path := shardMappingPath()
	maxCluster := getClusterLayout().ClusterCount
	raw := make(map[string]int, len(overrides))
	for key, clusterID := range overrides {
		if clusterID < 1 || maxCluster > 0 && clusterID > maxCluster {
			continue
		}
		raw[strconv.Itoa(key)] = clusterID
	}
	bytes, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	if err := os.WriteFile(path, bytes, 0644); err != nil {
		return err
	}
	info, err := os.Stat(path)
	if err == nil {
		shardOverridesMu.Lock()
		shardOverrides = make(map[int]int, len(overrides))
		for k, v := range overrides {
			if v >= 1 && (maxCluster == 0 || v <= maxCluster) {
				shardOverrides[k] = v
			}
		}
		shardOverridesClock = info.ModTime()
		shardOverridesMu.Unlock()
	}
	return nil
}

func ClearShardOverrides() error {
	path := shardMappingPath()
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	shardOverridesMu.Lock()
	shardOverrides = nil
	shardOverridesClock = time.Time{}
	shardOverridesMu.Unlock()
	return nil
}

func GetClusterForKey(key int) *Cluster {
	clusterID := ResolveClusterIDForKey(key)
	if clusterID == 0 {
		return nil
	}
	leader := GetClusterLeaderID(clusterID)
	return &Cluster{Id: clusterID, Leader: leader}
}

func NewCluster(id int) *Cluster {
	leaderID := GetClusterLeaderID(id)
	capacity := NodesPerCluster()
	if capacity <= 0 {
		capacity = 1
	}
	return &Cluster{
		Id:     id,
		Nodes:  make([]*Node, 0, capacity),
		Leader: leaderID,
	}
}

func GetClusterLeaderID(clusterId int) int {
	layout := getClusterLayout()
	if clusterId < 1 || clusterId > layout.ClusterCount || layout.NodesPerCluster <= 0 {
		return 0
	}
	return (clusterId-1)*layout.NodesPerCluster + 1
}

func GetNodePort(nodeId int) int {
	return 8000 + nodeId
}

func GetClusterNodeIDs(clusterId int) []int {
	layout := getClusterLayout()
	if clusterId < 1 || clusterId > layout.ClusterCount || layout.NodesPerCluster <= 0 {
		return nil
	}
	start := (clusterId-1)*layout.NodesPerCluster + 1
	ids := make([]int, 0, layout.NodesPerCluster)
	for i := 0; i < layout.NodesPerCluster; i++ {
		ids = append(ids, start+i)
	}
	return ids
}

func GetNodeConfig(nodeID int) (int, int, bool) {
	layout := getClusterLayout()
	if nodeID < 1 || layout.NodesPerCluster <= 0 || layout.ClusterCount <= 0 {
		return 0, 0, false
	}
	totalNodes := layout.ClusterCount * layout.NodesPerCluster
	if nodeID > totalNodes {
		return 0, 0, false
	}
	clusterId := (nodeID-1)/layout.NodesPerCluster + 1
	return nodeID, clusterId, true
}
