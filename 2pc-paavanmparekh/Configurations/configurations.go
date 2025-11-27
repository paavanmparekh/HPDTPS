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

	// Client/txn tracking for deduplication with int client IDs (0..8999)
	ClientLastReply   map[int]int      // clientID -> last timestamp
	TxnsProcessed     map[string]Reply // sender:timestamp -> reply
	T                 time.Duration    // election timeout
	Tp                time.Duration    // proposal timeout
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
	for keyStr, clusterID := range raw {
		keyInt, err := strconv.Atoi(keyStr)
		if err != nil {
			continue
		}
		updated[keyInt] = clusterID
	}
	shardOverridesMu.Lock()
	shardOverrides = updated
	shardOverridesClock = info.ModTime()
	shardOverridesMu.Unlock()
}

func DefaultClusterForKey(key int) int {
	switch {
	case key >= 1 && key <= 3000:
		return 1
	case key >= 3001 && key <= 6000:
		return 2
	case key >= 6001 && key <= 9000:
		return 3
	default:
		return 0
	}
}

func ResolveClusterIDForKey(key int) int {
	maybeReloadShardOverrides()
	shardOverridesMu.RLock()
	if shardOverrides != nil {
		if cid, ok := shardOverrides[key]; ok && cid >= 1 && cid <= 3 {
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
	raw := make(map[string]int, len(overrides))
	for key, clusterID := range overrides {
		if clusterID < 1 || clusterID > 3 {
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
			shardOverrides[k] = v
		}
		shardOverridesClock = info.ModTime()
		shardOverridesMu.Unlock()
	}
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
	return &Cluster{
		Id:     id,
		Nodes:  make([]*Node, 0, 3),
		Leader: leaderID,
	}
}

func GetClusterLeaderID(clusterId int) int {
	return (clusterId-1)*3 + 1
}

func GetNodePort(nodeId int) int {
	return 8000 + nodeId
}

func GetClusterNodeIDs(clusterId int) []int {
	start := (clusterId-1)*3 + 1
	return []int{start, start + 1, start + 2}
}

func GetNodeConfig(nodeID int) (int, int, bool) {
	nodeConfigs := map[int]struct {
		id        int
		clusterId int
	}{
		1: {1, 1}, 2: {2, 1}, 3: {3, 1},
		4: {4, 2}, 5: {5, 2}, 6: {6, 2},
		7: {7, 3}, 8: {8, 3}, 9: {9, 3},
	}

	cfg, exists := nodeConfigs[nodeID]
	return cfg.id, cfg.clusterId, exists
}
