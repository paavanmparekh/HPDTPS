package consensus

import (
	configurations "2pc-paavanmparekh/Configurations"
	commitment "2pc-paavanmparekh/Node/Commitment"
	nodelogger "2pc-paavanmparekh/Node/logger"
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	participantDecisionTimeout = 1000 * time.Millisecond
	twoPCStateRetention        = 6000 * time.Millisecond
	terminationRetryInterval   = 1200 * time.Millisecond
	peerRPCPoolSize            = 16
)

type rpcClientSlot struct {
	mu     sync.Mutex
	client *rpc.Client
}

type rpcPooledClient struct {
	mu    sync.Mutex
	slots []*rpcClientSlot
	next  int
}

// NodeService hosts the Multi-Paxos RPCs for a node within its shard/cluster.
type NodeService struct {
	Node                   *configurations.Node
	ElectionTimer          *time.Timer
	TpTimer                *time.Timer
	isNewView              bool
	StartupPhase           bool
	mu                     sync.RWMutex
	executionResults       map[int]configurations.TxnReply
	resultsMutex           sync.RWMutex
	PendingClientResponses map[int]chan configurations.TxnReply
	pendingMutex           sync.RWMutex
	txnStatusMu            sync.RWMutex
	executionMu            sync.Mutex
	WaitingRequests        map[int]bool
	TimerRunning           bool
	TimerExpired           bool
	ForceElection          bool
	locks                  map[int]string // key -> owner derived from txn fields
	logger                 *nodelogger.Logger
	modifiedKeys           map[int]bool
	modifiedKeysMutex      sync.RWMutex
	newViewMessages        []configurations.NewViewInput
	newViewMu              sync.RWMutex
	wal                    *commitment.WALManager
	twoPCMu                sync.RWMutex
	twoPCState             map[string]*commitment.TransactionState
	cacheMu                sync.RWMutex
	balanceCache           map[int]int
	benchmarkMode          bool
	stmtSelect             *sql.Stmt
	stmtDebit              *sql.Stmt
	stmtCredit             *sql.Stmt
	peerClientPool         struct {
		sync.Mutex
		clients map[int]*rpcPooledClient
	}
}

func NewNodeService(node *configurations.Node) *NodeService {
	logger := nodelogger.GetLogger(node.Id)
	wal := commitment.NewWALManager(node.Db)
	if err := wal.EnsureTables(); err != nil {
		logger.Log("[Node %d] WAL initialization failed: %v\n", node.Id, err)
	}
	svc := &NodeService{
		Node:                   node,
		StartupPhase:           true,
		executionResults:       make(map[int]configurations.TxnReply),
		PendingClientResponses: make(map[int]chan configurations.TxnReply),
		WaitingRequests:        make(map[int]bool),
		locks:                  make(map[int]string),
		logger:                 logger,
		modifiedKeys:           make(map[int]bool),
		wal:                    wal,
		twoPCState:             make(map[string]*commitment.TransactionState),
		balanceCache:           make(map[int]int),
	}
	svc.peerClientPool.clients = make(map[int]*rpcPooledClient)
	svc.ElectionTimer = time.NewTimer(node.T)
	if svc.ElectionTimer.Stop() {
	}
	svc.TpTimer = time.NewTimer(node.Tp)
	if err := svc.prepareStatements(); err != nil {
		logger.Log("[Node %d] Statement preparation failed: %v\n", node.Id, err)
	}
	go svc.startElectionLoop()
	return svc
}

func (n *NodeService) prepareStatements() error {
	if n.Node == nil || n.Node.Db == nil {
		return nil
	}
	var err error
	if n.stmtSelect, err = n.Node.Db.Prepare("SELECT balance FROM balances WHERE key = ?"); err != nil {
		return err
	}
	if n.stmtDebit, err = n.Node.Db.Prepare("UPDATE balances SET balance = balance - ? WHERE key = ?"); err != nil {
		return err
	}
	if n.stmtCredit, err = n.Node.Db.Prepare("UPDATE balances SET balance = balance + ? WHERE key = ?"); err != nil {
		return err
	}
	return nil
}

func (n *NodeService) benchmarkDurationsEnabled() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.benchmarkMode
}

func (n *NodeService) participantDecisionTimeoutValue() time.Duration {
	// Use a short, fixed timeout to keep termination quick under load.
	return 150 * time.Millisecond
}

func (n *NodeService) terminationRetryIntervalValue() time.Duration {
	// Retry termination frequently to avoid long pauses.
	return 200 * time.Millisecond
}

func (n *NodeService) participantRetryDelay() time.Duration {
	// Keep retries snappy to reduce end-to-end latency.
	return 20 * time.Millisecond
}

func (n *NodeService) startElectionLoop() {
	for {
		<-n.ElectionTimer.C

		n.mu.Lock()
		forced := n.ForceElection
		if forced {
			n.ForceElection = false
		}
		timerRunning := n.TimerRunning
		waiting := len(n.WaitingRequests)
		if timerRunning && waiting > 0 {
			n.TimerExpired = true
		}
		startup := n.StartupPhase
		isLive := n.Node.IsLive
		n.mu.Unlock()

		if !forced && (!timerRunning || waiting == 0) {
			continue
		}
		if !startup && !isLive {
			continue
		}

		n.mu.Lock()
		pb := configurations.BallotNumber{B: n.Node.Bnum.B + 1, NodeID: n.Node.Id}
		n.Node.Bnum = pb
		n.mu.Unlock()
		n.logger.Log("[Node %d] Election timeout, starting election with ballot %d\n", pb.NodeID, pb.B)

		select {
		case <-n.TpTimer.C:
			n.BroadcastPrepareRPC(pb)
			n.TpTimer.Reset(n.Node.Tp)
		default:
			n.logger.Log("[Node %d] Skip election, waiting for Tp timeout\n", n.Node.Id)
		}
	}
}

func (n *NodeService) triggerElection(reason string) {
	if !n.Node.IsLive {
		return
	}
	n.mu.Lock()
	n.ForceElection = true
	n.mu.Unlock()
	if reason != "" {
		n.logger.Log("[Node %d] ELECTION: Triggered (%s)\n", n.Node.Id, reason)
	}
	n.ElectionTimer.Reset(0)
}

// clusterNodes returns the node IDs in this node's cluster.
func (n *NodeService) clusterNodes() []int {
	return configurations.GetClusterNodeIDs(n.Node.ClusterId)
}

func majority(count int) int {
	return count/2 + 1
}

func (n *NodeService) BroadcastToCluster(handler func(targetNode int)) {
	for _, targetNode := range n.clusterNodes() {
		if targetNode == n.Node.Id {
			continue
		}
		go handler(targetNode)
	}
}

func (p *rpcPooledClient) pickSlot() *rpcClientSlot {
	p.mu.Lock()
	slot := p.slots[p.next]
	p.next = (p.next + 1) % len(p.slots)
	p.mu.Unlock()
	return slot
}

func (n *NodeService) getPeerClient(nodeID int) (*rpcPooledClient, error) {
	n.peerClientPool.Lock()
	if pooled := n.peerClientPool.clients[nodeID]; pooled != nil {
		n.peerClientPool.Unlock()
		return pooled, nil
	}
	slots := make([]*rpcClientSlot, peerRPCPoolSize)
	for i := range slots {
		slots[i] = &rpcClientSlot{}
	}
	pooled := &rpcPooledClient{slots: slots}
	n.peerClientPool.clients[nodeID] = pooled
	n.peerClientPool.Unlock()
	return pooled, nil
}

func (n *NodeService) dialPeer(nodeID int) (*rpc.Client, error) {
	port := configurations.GetNodePort(nodeID)
	if port == 0 {
		return nil, fmt.Errorf("no port for node %d", nodeID)
	}
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to node %d: %w", nodeID, err)
	}
	return client, nil
}

func (n *NodeService) callPeerRPC(nodeID int, method string, req interface{}, resp interface{}) error {
	pooled, err := n.getPeerClient(nodeID)
	if err != nil {
		return err
	}
	slot := pooled.pickSlot()
	slot.mu.Lock()
	if slot.client == nil {
		client, err := n.dialPeer(nodeID)
		if err != nil {
			slot.mu.Unlock()
			return err
		}
		slot.client = client
	}
	client := slot.client
	if err := client.Call(method, req, resp); err != nil {
		if slot.client == client {
			slot.client.Close()
			slot.client = nil
		}
		slot.mu.Unlock()
		return fmt.Errorf("%s RPC to node %d failed: %w", method, nodeID, err)
	}
	slot.mu.Unlock()
	return nil
}

func (n *NodeService) resetPeerClientPool() {
	n.peerClientPool.Lock()
	for _, pooled := range n.peerClientPool.clients {
		for _, slot := range pooled.slots {
			slot.mu.Lock()
			if slot.client != nil {
				slot.client.Close()
				slot.client = nil
			}
			slot.mu.Unlock()
		}
	}
	n.peerClientPool.clients = make(map[int]*rpcPooledClient)
	n.peerClientPool.Unlock()
}

// --- Utility helpers ---

func txnDedupKey(txn configurations.Transaction) string {
	// Use sender + logical timestamp + receiver/amount to distinguish retries
	return fmt.Sprintf("%d:%d:%d:%d:%t:%t", txn.Sender, txn.Receiver, txn.Amount, txn.Timestamp, txn.ReadOnly, txn.CreditOnly)
}

func lockOwnerKey(txn configurations.Transaction) string {
	return fmt.Sprintf("%d:%d:%d:%d:%t:%t", txn.Sender, txn.Timestamp, txn.Receiver, txn.Amount, txn.ReadOnly, txn.CreditOnly)
}

func txnIdentity(txn configurations.Transaction) string {
	return fmt.Sprintf("%d:%d:%d:%t", txn.Sender, txn.Receiver, txn.Timestamp, txn.ReadOnly)
}

func phaseLabel(phase configurations.TwoPCPhase) string {
	switch phase {
	case configurations.PhasePrepare:
		return "PREPARE"
	case configurations.PhaseCommit:
		return "COMMIT"
	case configurations.PhaseAbort:
		return "ABORT"
	default:
		return "UNKNOWN"
	}
}

func isTransientReply(msg string) bool {
	switch msg {
	case "Locked", "LeaderUnknown", "NotLeader", "Majority Not Accepted":
		return true
	}
	if strings.HasPrefix(msg, "ParticipantPrepare") {
		return true
	}
	return false
}

func (n *NodeService) getCachedBalance(key int) (int, bool) {
	n.cacheMu.RLock()
	defer n.cacheMu.RUnlock()
	val, ok := n.balanceCache[key]
	return val, ok
}

func (n *NodeService) setCachedBalance(key int, value int) {
	n.cacheMu.Lock()
	n.balanceCache[key] = value
	n.cacheMu.Unlock()
}

func (n *NodeService) invalidateBalance(key int) {
	n.cacheMu.Lock()
	delete(n.balanceCache, key)
	n.cacheMu.Unlock()
}

func (n *NodeService) invalidateTxnKeys(txn configurations.Transaction) {
	n.invalidateBalance(txn.Sender)
	if txn.Receiver != txn.Sender {
		n.invalidateBalance(txn.Receiver)
	}
}

const (
	sqliteBusyRetries = 8
	sqliteBusyDelay   = 20 * time.Millisecond
)

func isSQLiteBusy(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "database is locked") || strings.Contains(msg, "database is busy")
}

func (n *NodeService) acquireLocks(txn configurations.Transaction) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	owner := lockOwnerKey(txn)
	if current, ok := n.locks[txn.Sender]; ok && current != owner {
		return false
	}
	if current, ok := n.locks[txn.Receiver]; ok && current != owner {
		return false
	}
	n.locks[txn.Sender] = owner
	n.locks[txn.Receiver] = owner
	return true
}

func (n *NodeService) releaseLocks(txn configurations.Transaction) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.locks, txn.Sender)
	delete(n.locks, txn.Receiver)
}

// hasLockConflict reports whether either account touched by txn is held by another owner.
func (n *NodeService) hasLockConflict(txn configurations.Transaction) bool {
	owner := lockOwnerKey(txn)
	n.mu.RLock()
	defer n.mu.RUnlock()
	if current, ok := n.locks[txn.Sender]; ok && current != owner {
		return true
	}
	if txn.Receiver != txn.Sender {
		if current, ok := n.locks[txn.Receiver]; ok && current != owner {
			return true
		}
	}
	return false
}

func (n *NodeService) cleanupTwoPCStatesLocked() {
	now := time.Now()
	for txnID, state := range n.twoPCState {
		if state.AcknowledgedDecision && !state.CleanupAt.IsZero() && now.After(state.CleanupAt) {
			delete(n.twoPCState, txnID)
		}
	}
}

func (n *NodeService) getTwoPCState(txnID string) *commitment.TransactionState {
	n.twoPCMu.Lock()
	defer n.twoPCMu.Unlock()
	n.cleanupTwoPCStatesLocked()
	if state, ok := n.twoPCState[txnID]; ok {
		return state
	}
	state := &commitment.TransactionState{
		TxnID:     txnID,
		CreatedAt: time.Now(),
	}
	n.twoPCState[txnID] = state
	return state
}

func (n *NodeService) loadTwoPCState(txnID string) (*commitment.TransactionState, bool) {
	n.twoPCMu.RLock()
	defer n.twoPCMu.RUnlock()
	state, ok := n.twoPCState[txnID]
	return state, ok
}

func (n *NodeService) clearTwoPCState(txnID string) {
	n.twoPCMu.Lock()
	if state, ok := n.twoPCState[txnID]; ok {
		if state.Timer != nil {
			state.Timer.Stop()
			state.Timer = nil
		}
		state.CleanupAt = time.Now().Add(twoPCStateRetention)
		state.AcknowledgedDecision = true
	}
	n.cleanupTwoPCStatesLocked()
	n.twoPCMu.Unlock()
}

func (n *NodeService) buildTwoPCStatus(txnID string) (commitment.TwoPCStatusResponse, bool) {
	status := commitment.TwoPCStatusResponse{
		TxnID:    txnID,
		Decision: configurations.PhaseNone,
		Phase:    configurations.PhaseNone,
	}
	n.mu.RLock()
	for i := len(n.Node.AcceptLog) - 1; i >= 0; i-- {
		entry := n.Node.AcceptLog[i]
		if entry.TxnID != txnID {
			continue
		}
		status.Known = true
		status.Phase = entry.Phase
		status.Role = entry.Role
		status.SeqNo = entry.AcceptSeq
		status.CoordinatorCluster = entry.CoordinatorCluster
		status.ParticipantCluster = entry.ParticipantCluster
		if entry.Phase == configurations.PhaseCommit || entry.Phase == configurations.PhaseAbort {
			status.Decision = entry.Phase
			n.mu.RUnlock()
			return status, true
		}
		if status.Decision == configurations.PhaseNone {
			status.Decision = entry.Phase
		}
	}
	n.mu.RUnlock()

	n.twoPCMu.RLock()
	if state, ok := n.twoPCState[txnID]; ok {
		status.Known = true
		status.Phase = state.Phase
		if state.Decision != configurations.PhaseNone {
			status.Decision = state.Decision
		}
		status.Role = state.Role
		status.CoordinatorCluster = state.CoordinatorCluster
		status.ParticipantCluster = state.ParticipantCluster
		if state.Role == configurations.RoleCoordinator {
			status.SeqNo = state.CoordinatorSeq
		} else if state.Role == configurations.RoleParticipant {
			status.SeqNo = state.ParticipantSeq
		}
	}
	n.twoPCMu.RUnlock()
	return status, status.Known
}

func (n *NodeService) callQueryTwoPCRPC(nodeID int, req commitment.TwoPCStatusRequest, resp *commitment.TwoPCStatusResponse) error {
	return n.callPeerRPC(nodeID, "Node.QueryTwoPCState", req, resp)
}

func (n *NodeService) queryClusterForDecision(clusterID int, txnID string) *commitment.TwoPCStatusResponse {
	if clusterID == 0 {
		return nil
	}
	nodeIDs := configurations.GetClusterNodeIDs(clusterID)
	req := commitment.TwoPCStatusRequest{TxnID: txnID}
	for _, nodeID := range nodeIDs {
		var resp commitment.TwoPCStatusResponse
		var err error
		if nodeID == n.Node.Id {
			var ok bool
			resp, ok = n.buildTwoPCStatus(txnID)
			if !ok {
				continue
			}
		} else {
			err = n.callQueryTwoPCRPC(nodeID, req, &resp)
		}
		if err != nil {
			n.logger.Log("[2PC] Node %d: QUERY txnID=%s node=%d failed: %v\n", n.Node.Id, txnID, nodeID, err)
			continue
		}
		if resp.Decision == configurations.PhaseCommit || resp.Decision == configurations.PhaseAbort {
			return &resp
		}
	}
	return nil
}

func (n *NodeService) applyTerminationDecision(state *commitment.TransactionState, decision configurations.TwoPCPhase) bool {
	if decision != configurations.PhaseCommit && decision != configurations.PhaseAbort {
		return false
	}
	acceptObj := configurations.AcceptTxn{
		Txn:                state.Txn,
		Phase:              decision,
		Role:               configurations.RoleParticipant,
		TxnID:              state.TxnID,
		Status:             "2pc-termination",
		IsCrossShard:       true,
		CoordinatorCluster: state.CoordinatorCluster,
		ParticipantCluster: state.ParticipantCluster,
	}
	if state.ParticipantSeq > 0 {
		acceptObj.SeqNo = state.ParticipantSeq
	}
	res := n.broadcastCustomAccept(&acceptObj)
	return res != nil && res.Res
}

func (n *NodeService) scheduleTerminationRetry(txnID string) {
	n.twoPCMu.Lock()
	state, ok := n.twoPCState[txnID]
	if !ok {
		n.twoPCMu.Unlock()
		return
	}
	if state.Timer != nil {
		state.Timer.Stop()
	}
	state.Timer = time.AfterFunc(n.terminationRetryIntervalValue(), func() {
		n.runParticipantTermination(txnID)
	})
	n.twoPCMu.Unlock()
}

func (n *NodeService) runParticipantTermination(txnID string) {
	state, ok := n.loadTwoPCState(txnID)
	if !ok {
		return
	}
	n.twoPCMu.Lock()
	if state.Timer != nil {
		state.Timer.Stop()
		state.Timer = nil
	}
	n.twoPCMu.Unlock()
	if state.Role != configurations.RoleParticipant || state.Decision != configurations.PhaseNone || state.Phase != configurations.PhasePrepare {
		return
	}

	localStatus, localKnown := n.buildTwoPCStatus(txnID)
	if localKnown && (localStatus.Decision == configurations.PhaseCommit || localStatus.Decision == configurations.PhaseAbort) {
		if n.applyTerminationDecision(state, localStatus.Decision) {
			return
		}
	}
	if resp := n.queryClusterForDecision(state.ParticipantCluster, txnID); resp != nil {
		if n.applyTerminationDecision(state, resp.Decision) {
			return
		}
	}
	if resp := n.queryClusterForDecision(state.CoordinatorCluster, txnID); resp != nil {
		if n.applyTerminationDecision(state, resp.Decision) {
			return
		}
	}

	n.logger.Log("[2PC] Node %d: Termination protocol blocked txnID=%s waiting for coordinator recovery\n", n.Node.Id, txnID)
	n.scheduleTerminationRetry(txnID)
}

func (n *NodeService) markModifiedKey(key int) {
	n.modifiedKeysMutex.Lock()
	n.modifiedKeys[key] = true
	n.modifiedKeysMutex.Unlock()
}

func (n *NodeService) startParticipantDecisionTimer(txnID string, txn configurations.Transaction) {
	timeout := n.participantDecisionTimeoutValue()
	n.twoPCMu.Lock()
	state, ok := n.twoPCState[txnID]
	if !ok {
		state = &commitment.TransactionState{TxnID: txnID}
		n.twoPCState[txnID] = state
	}
	if state.AcknowledgedDecision || state.Decision != configurations.PhaseNone {
		n.twoPCMu.Unlock()
		return
	}
	if state.Timer != nil {
		state.Timer.Stop()
	}
	state.Txn = txn
	state.Deadline = time.Now().Add(timeout)
	state.Timer = time.AfterFunc(timeout, func() {
		n.handleParticipantDecisionTimeout(txnID)
	})
	n.twoPCMu.Unlock()
	n.logger.Log("[2PC] Node %d: Participant decision timer started txnID=%s timeout=%v\n", n.Node.Id, txnID, timeout)
}

func (n *NodeService) handleParticipantDecisionTimeout(txnID string) {
	if !n.Node.IsLive {
		return
	}
	n.logger.Log("[2PC] Node %d: Participant decision timeout txnID=%s -> starting termination protocol\n", n.Node.Id, txnID)
	n.runParticipantTermination(txnID)
}

func (n *NodeService) stopAllTwoPCTimers() {
	n.twoPCMu.Lock()
	for _, state := range n.twoPCState {
		if state.Timer != nil {
			state.Timer.Stop()
			state.Timer = nil
		}
	}
	n.twoPCMu.Unlock()
}

func (n *NodeService) resumeTwoPCTimers() {
	type participantInfo struct {
		txnID string
		txn   configurations.Transaction
	}
	var participants []participantInfo
	n.twoPCMu.Lock()
	for txnID, state := range n.twoPCState {
		if state.Timer != nil {
			state.Timer.Stop()
			state.Timer = nil
		}
		if state.Role == configurations.RoleParticipant &&
			state.Phase == configurations.PhasePrepare &&
			state.Decision == configurations.PhaseNone {
			participants = append(participants, participantInfo{txnID: txnID, txn: state.Txn})
		}
	}
	n.twoPCMu.Unlock()
	for _, p := range participants {
		n.startParticipantDecisionTimer(p.txnID, p.txn)
	}
}

func (n *NodeService) PrintDB() {
	if n.Node.Db == nil {
		fmt.Printf("[Node %d] Database not initialized\n", n.Node.Id)
		return
	}
	n.modifiedKeysMutex.RLock()
	if len(n.modifiedKeys) == 0 {
		n.modifiedKeysMutex.RUnlock()
		fmt.Printf("[Node %d] DB: <no keys modified>\n", n.Node.Id)
		return
	}
	keys := make([]int, 0, len(n.modifiedKeys))
	for k := range n.modifiedKeys {
		keys = append(keys, k)
	}
	n.modifiedKeysMutex.RUnlock()
	sort.Ints(keys)

	pairs := make([]string, 0, len(keys))
	for _, key := range keys {
		var balance int
		if err := n.Node.Db.QueryRow("SELECT balance FROM balances WHERE key = ?", key).Scan(&balance); err != nil {
			if err == sql.ErrNoRows {
				continue
			}
			fmt.Printf("[Node %d] Failed to read key %d: %v\n", n.Node.Id, key, err)
			continue
		}
		pairs = append(pairs, fmt.Sprintf("%d:%d", key, balance))
	}
	fmt.Printf("[Node %d] DB: %s\n", n.Node.Id, strings.Join(pairs, ", "))
}

func (n *NodeService) GetTransactionStatus(seq int) string {
	n.txnStatusMu.RLock()
	defer n.txnStatusMu.RUnlock()
	if status, ok := n.Node.TransactionStatus[seq]; ok {
		return status
	}
	return "unknown"
}

func (n *NodeService) ensureTxnStatus(seq int, status string) {
	n.txnStatusMu.Lock()
	if _, ok := n.Node.TransactionStatus[seq]; !ok {
		n.Node.TransactionStatus[seq] = status
	}
	n.txnStatusMu.Unlock()
}

func (n *NodeService) setTxnStatus(seq int, status string) {
	n.txnStatusMu.Lock()
	n.Node.TransactionStatus[seq] = status
	n.txnStatusMu.Unlock()
}

func (n *NodeService) resetTxnStatus() {
	n.txnStatusMu.Lock()
	n.Node.TransactionStatus = make(map[int]string)
	n.txnStatusMu.Unlock()
}

func (n *NodeService) PrintView() {
	n.newViewMu.RLock()
	messages := make([]configurations.NewViewInput, len(n.newViewMessages))
	copy(messages, n.newViewMessages)
	n.newViewMu.RUnlock()
	n.mu.RLock()
	currentBallot := n.Node.Bnum
	lastExecuted := n.Node.LastExecuted
	n.mu.RUnlock()
	fmt.Printf("[Node %d] Current ballot=%v, LastExecuted=%d\n", n.Node.Id, currentBallot, lastExecuted)
	if len(messages) == 0 {
		fmt.Printf("[Node %d] No new-view messages recorded yet\n", n.Node.Id)
		return
	}
	fmt.Printf("[Node %d] New-view messages since start:\n", n.Node.Id)
	for idx, msg := range messages {
		fmt.Printf("NewView #%d: ballot=%v entries=%d\n", idx+1, msg.B, len(msg.AcceptLog))
		for _, entry := range msg.AcceptLog {
			txn := entry.AcceptVal
			fmt.Printf("  Seq %d: ballot=%v status=%s txn=%d->%d amt=%d readOnly=%t\n",
				entry.AcceptSeq, entry.AcceptNum, entry.Status,
				txn.Sender, txn.Receiver, txn.Amount, txn.ReadOnly)
		}
	}
}

func (n *NodeService) recordNewViewMessage(b configurations.BallotNumber, acceptLog []configurations.AcceptLog) {
	n.newViewMu.Lock()
	defer n.newViewMu.Unlock()
	entry := configurations.NewViewInput{B: b}
	if len(acceptLog) > 0 {
		entry.AcceptLog = make([]configurations.AcceptLog, len(acceptLog))
		copy(entry.AcceptLog, acceptLog)
	}
	n.newViewMessages = append(n.newViewMessages, entry)
}

func (n *NodeService) PrintBalance(key int) {
	cluster := configurations.GetClusterForKey(key)
	if cluster == nil {
		fmt.Printf("[Node %d] PrintBalance(%d); Output: invalid key\n", n.Node.Id, key)
		return
	}
	nodeIDs := configurations.GetClusterNodeIDs(cluster.Id)
	results := make([]string, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		balance, err := n.fetchBalanceFromNode(nodeID, key)
		if err != nil {
			results = append(results, fmt.Sprintf("n%d : %s", nodeID, err.Error()))
			continue
		}
		results = append(results, fmt.Sprintf("n%d : %d", nodeID, balance))
	}
	fmt.Printf("PrintBalance(%d); Output: %s\n", key, strings.Join(results, ", "))
}

func (n *NodeService) readLocalBalance(key int) (int, error) {
	if n.Node.Db == nil {
		return 0, fmt.Errorf("database not initialized")
	}
	var balance int
	if err := n.Node.Db.QueryRow("SELECT balance FROM balances WHERE key = ?", key).Scan(&balance); err != nil {
		return 0, err
	}
	return balance, nil
}

func (n *NodeService) fetchBalanceFromNode(nodeID, key int) (int, error) {
	if nodeID == n.Node.Id {
		return n.readLocalBalance(key)
	}
	var balance int
	if err := n.callPeerRPC(nodeID, "Node.GetBalance", key, &balance); err != nil {
		return 0, err
	}
	return balance, nil
}

func (n *NodeService) GetBalance(key int, reply *int) error {
	balance, err := n.readLocalBalance(key)
	if err != nil {
		return err
	}
	*reply = balance
	return nil
}

func (n *NodeService) SetBalance(update configurations.BalanceUpdate, ack *bool) error {
	if n.Node.Db == nil {
		return fmt.Errorf("database not initialized")
	}
	_, err := n.Node.Db.Exec("INSERT INTO balances(key, balance) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET balance = excluded.balance", update.Key, update.Balance)
	if err != nil {
		return err
	}
	n.modifiedKeysMutex.Lock()
	n.modifiedKeys[update.Key] = true
	n.modifiedKeysMutex.Unlock()
	*ack = true
	return nil
}

func (n *NodeService) DeleteBalance(key int, ack *bool) error {
	if n.Node.Db == nil {
		return fmt.Errorf("database not initialized")
	}
	if _, err := n.Node.Db.Exec("DELETE FROM balances WHERE key = ?", key); err != nil {
		return err
	}
	n.modifiedKeysMutex.Lock()
	delete(n.modifiedKeys, key)
	n.modifiedKeysMutex.Unlock()
	*ack = true
	return nil
}

// --- RPC Helpers copied and adapted from Project1 ---

func (n *NodeService) BroadcastNewViewRPC(B configurations.BallotNumber, acceptLog []configurations.AcceptLog) {
	var mu sync.Mutex
	var wg sync.WaitGroup

	CountAcceptedMsgs := make(map[int]int)
	SeqToTxnMap := make(map[int]configurations.AcceptTxn)
	n.logger.Log("[PAXOS] Node %d: NEW-VIEW start ballot=%v entries=%d\n", B.NodeID, B, len(acceptLog))
	n.recordNewViewMessage(B, acceptLog)

	for _, targetNode := range n.clusterNodes() {
		if targetNode == B.NodeID {
			continue
		}
		var newViewInput configurations.NewViewInput
		newViewInput.B = B
		newViewInput.AcceptLog = acceptLog
		for i := range newViewInput.AcceptLog {
			newViewInput.AcceptLog[i].AcceptNum = B
		}

		wg.Add(1)
		go func(target int) {
			defer wg.Done()
			var reply map[int]configurations.AcceptTxn
			if err := n.callPeerRPC(target, "Node.NewView", newViewInput, &reply); err != nil {
				n.logger.Log("[Node %v] NewView RPC to node %d failed: %v\n", B.NodeID, target, err)
				return
			}
			mu.Lock()
			for key, value := range reply {
				CountAcceptedMsgs[key] += value.Acceptance
				SeqToTxnMap[key] = value
			}
			mu.Unlock()
		}(targetNode)
	}
	wg.Wait()
	n.isNewView = false
	for key, val := range CountAcceptedMsgs {
		if val+1 >= majority(len(n.clusterNodes())) {
			n.logger.Log("[Node %v] Majority Accepted Txn SeqNo: %v, committing now...\n", B.NodeID, key)
			n.BroadcastCommitRPC(SeqToTxnMap[key])
		} else {
			n.logger.Log("[Node %v] Txn SeqNo %v not accepted by majority\n", B.NodeID, key)
		}
	}
}

func (n *NodeService) BroadcastPrepareRPC(PB configurations.BallotNumber) {
	votes := 0
	acceptMap := make(map[int]struct {
		Ballot configurations.BallotNumber
		Txn    configurations.Transaction
	})

	var mu sync.Mutex
	var wg sync.WaitGroup
	n.logger.Log("[Node %v] Sending Prepare RPC to cluster peers\n", PB.NodeID)
	n.logger.Log("[PAXOS] Node %d: PREPARE start ballot=%v\n", PB.NodeID, PB)

	for _, targetNode := range n.clusterNodes() {
		if targetNode == PB.NodeID {
			continue
		}

		wg.Add(1)
		go func(target int) {
			defer wg.Done()
			var reply configurations.Promise
			if err := n.callPeerRPC(target, "Node.Prepare", PB, &reply); err != nil {
				n.logger.Log("[Node %v] Prepare RPC to node %d failed: %v\n", PB.NodeID, target, err)
				return
			}
			mu.Lock()
			votes += reply.Vote
			if reply.Vote == 1 && len(reply.AcceptedMsgs) != 0 {
				ReceivedAcceptLog := reply.AcceptedMsgs
				for _, msg := range ReceivedAcceptLog {
					if entry, exists := acceptMap[msg.AcceptSeq]; !exists {
						acceptMap[msg.AcceptSeq] = struct {
							Ballot configurations.BallotNumber
							Txn    configurations.Transaction
						}{msg.AcceptNum, msg.AcceptVal}
					} else if msg.AcceptNum.B >= entry.Ballot.B {
						acceptMap[msg.AcceptSeq] = struct {
							Ballot configurations.BallotNumber
							Txn    configurations.Transaction
						}{msg.AcceptNum, msg.AcceptVal}
					}
				}
			}
			mu.Unlock()
		}(targetNode)
	}
	wg.Wait()

	var acceptLog []configurations.AcceptLog
	maxSequenceNum := -1
	for k := range acceptMap {
		if k > maxSequenceNum {
			maxSequenceNum = k
		}
	}

	n.logger.Log("[Node %v] Received %v votes\n", PB.NodeID, votes)
	if votes+1 >= majority(len(n.clusterNodes())) {
		n.logger.Log("[PAXOS] Node %d: PREPARE majority ballot=%v\n", PB.NodeID, PB)
		n.logger.Log("[Node %v] Elected as leader, checking NEW-VIEW\n", PB.NodeID)
		if maxSequenceNum >= 0 {
			n.Node.SequenceNumber = max(n.Node.SequenceNumber, maxSequenceNum)
		}
		for i := 1; maxSequenceNum >= 1 && i <= maxSequenceNum; i++ {
			if _, ok := acceptMap[i]; !ok {
				acceptLog = append(acceptLog, configurations.AcceptLog{AcceptSeq: i, AcceptNum: configurations.BallotNumber{}, AcceptVal: configurations.Transaction{}, Status: "no-op"})
				continue
			}
			acceptLog = append(acceptLog, configurations.AcceptLog{AcceptSeq: i, AcceptNum: acceptMap[i].Ballot, AcceptVal: acceptMap[i].Txn, Status: "regular"})
		}
		if len(acceptLog) != 0 {
			n.isNewView = true
			n.logger.Log("[Node %v] Broadcasting NEW-VIEW RPCs %v\n", PB.NodeID, acceptLog)
			n.BroadcastNewViewRPC(PB, acceptLog)
		}
	} else {
		n.logger.Log("[Node %v] Lost election\n", PB.NodeID)
		n.logger.Log("[PAXOS] Node %d: PREPARE lost ballot=%v\n", PB.NodeID, PB)
		n.mu.Lock()
		if n.Node.Bnum.B < PB.B {
			n.Node.Bnum.B = PB.B
		}
		n.mu.Unlock()
	}
}

func (n *NodeService) BroadcastCommitRPC(acceptObj configurations.AcceptTxn) {
	n.logger.Log("[Node %d] LEADER: Starting commit phase for SeqNo %d\n", n.Node.Id, acceptObj.SeqNo)
	n.logger.Log("[PAXOS] Node %d: COMMIT start seq=%d\n", n.Node.Id, acceptObj.SeqNo)

	if n.Node.LastExecuted < acceptObj.SeqNo {
		var localReply configurations.TxnReply
		n.ExecuteSerially(acceptObj, &localReply)
	}
	n.ensureTxnStatus(acceptObj.SeqNo, "Committed")

	n.BroadcastToCluster(func(targetNode int) {
		var reply configurations.TxnReply
		if err := n.callPeerRPC(targetNode, "Node.Commit", acceptObj, &reply); err != nil {
			n.logger.Log("[Node %d] COMMIT RPC to Node %d failed: %v\n", n.Node.Id, targetNode, err)
			return
		}
		n.logger.Log("[Node %d] COMMIT RPC to Node %d successful\n", n.Node.Id, targetNode)
	})
	n.logger.Log("[PAXOS] Node %d: COMMIT broadcast done for seq=%d\n", n.Node.Id, acceptObj.SeqNo)
}

func (n *NodeService) BroadcastAcceptRPC(txn configurations.Transaction) *configurations.TxnReply {
	acceptObj := configurations.AcceptTxn{
		Txn:    txn,
		Status: "regular",
	}
	return n.broadcastCustomAccept(&acceptObj)
}

func (n *NodeService) broadcastCustomAccept(acceptObj *configurations.AcceptTxn) *configurations.TxnReply {
	acceptance := 0
	var mu sync.Mutex
	var wg sync.WaitGroup

	if acceptObj.B.B < n.Node.Bnum.B {
		acceptObj.B = n.Node.Bnum
	}
	txnReply := configurations.TxnReply{}
	n.mu.Lock()
	if acceptObj.B.B == 0 {
		acceptObj.B = n.Node.Bnum
	}
	if n.Node.SequenceNumber < n.Node.LastExecuted {
		n.Node.SequenceNumber = n.Node.LastExecuted
	}
	if acceptObj.SeqNo == 0 {
		if n.Node.SequenceNumber < n.Node.LastExecuted {
			n.Node.SequenceNumber = n.Node.LastExecuted
		}
		n.Node.SequenceNumber++
		acceptObj.SeqNo = n.Node.SequenceNumber
	}
	if acceptObj.Status == "" {
		acceptObj.Status = "regular"
	}
	n.logger.Log("[PAXOS] Node %d: ACCEPT start seq=%d ballot=%v txn=%v->%v amt=%d\n", n.Node.Id, acceptObj.SeqNo, acceptObj.B, acceptObj.Txn.Sender, acceptObj.Txn.Receiver, acceptObj.Txn.Amount)

	n.logger.Log("[Node %d] LEADER: Assigned SeqNo %d (LastExecuted=%d)\n", n.Node.Id, acceptObj.SeqNo, n.Node.LastExecuted)

	n.Node.AcceptLog = append(n.Node.AcceptLog, configurations.AcceptLog{
		AcceptNum:          acceptObj.B,
		AcceptSeq:          acceptObj.SeqNo,
		AcceptVal:          acceptObj.Txn,
		Status:             acceptObj.Status,
		Phase:              acceptObj.Phase,
		Role:               acceptObj.Role,
		TxnID:              acceptObj.TxnID,
		IsCrossShard:       acceptObj.IsCrossShard,
		CoordinatorCluster: acceptObj.CoordinatorCluster,
		ParticipantCluster: acceptObj.ParticipantCluster,
	})
	n.ensureTxnStatus(acceptObj.SeqNo, "Accepted")
	n.mu.Unlock()

	n.logger.Log("[Node %d] LEADER: Starting ACCEPT phase (Ballot: %v, SeqNo: %d)\n", n.Node.Id, n.Node.Bnum, acceptObj.SeqNo)
	for _, targetNode := range n.clusterNodes() {
		if targetNode == n.Node.Id {
			continue
		}

		wg.Add(1)
		go func(target int) {
			defer wg.Done()
			var reply configurations.AcceptTxn
			if err := n.callPeerRPC(target, "Node.Accept", *acceptObj, &reply); err != nil {
				n.logger.Log("[Node %d] ACCEPT RPC to Node %d failed: %v\n", n.Node.Id, target, err)
				return
			}
			n.logger.Log("[Node %d] ACCEPT RPC to Node %d returned: %d\n", n.Node.Id, target, reply.Acceptance)
			mu.Lock()
			acceptance += reply.Acceptance
			mu.Unlock()
		}(targetNode)
	}

	wg.Wait()
	n.logger.Log("[Node %d] ACCEPT phase complete: %d acceptances (need %d)\n", n.Node.Id, acceptance+1, majority(len(n.clusterNodes())))
	if acceptance+1 >= majority(len(n.clusterNodes())) {
		n.logger.Log("[PAXOS] Node %d: ACCEPT majority for seq=%d\n", n.Node.Id, acceptObj.SeqNo)
		n.logger.Log("[Node %d] MAJORITY ACHIEVED: Proceeding to COMMIT phase\n", n.Node.Id)
		n.ensureTxnStatus(acceptObj.SeqNo, "Committed")
		var txnRes configurations.TxnReply
		var wgExec sync.WaitGroup
		needsExec := n.Node.LastExecuted < acceptObj.SeqNo
		if !needsExec && acceptObj.IsCrossShard && acceptObj.Role != configurations.RoleNone && acceptObj.Phase != configurations.PhasePrepare {
			needsExec = true
		}
		if needsExec {
			wgExec.Add(1)
			go func() {
				n.ExecuteSerially(*acceptObj, &txnRes)
				wgExec.Done()
			}()
			wgExec.Wait()
		}
		n.resultsMutex.RLock()
		if storedResult, exists := n.executionResults[acceptObj.SeqNo]; exists {
			txnRes = storedResult
		}
		n.resultsMutex.RUnlock()

		if txnRes.Msg == "Gap Found" {
			n.logger.Log("[Node %d] LEADER: Gap detected, waiting for execution SeqNo %d\n", n.Node.Id, acceptObj.SeqNo)
			responseCh := make(chan configurations.TxnReply, 1)
			n.pendingMutex.Lock()
			n.PendingClientResponses[acceptObj.SeqNo] = responseCh
			n.pendingMutex.Unlock()

			maxWait := 3 * n.Node.T
			if maxWait <= 0 {
				maxWait = 10 * time.Second
			}
			tickInterval := n.Node.T / 2
			if tickInterval <= 0 {
				tickInterval = time.Second
			}
			ticker := time.NewTicker(tickInterval)
			timer := time.NewTimer(maxWait)
			waiting := true
			for waiting {
				select {
				case actualResult := <-responseCh:
					txnRes = actualResult
					n.logger.Log("[Node %d] LEADER: Received actual execution result SeqNo %d: (%v %v)\n", n.Node.Id, acceptObj.SeqNo, txnRes.Res, txnRes.Msg)
					waiting = false
				case <-ticker.C:
					n.logger.Log("[Node %d] LEADER: Still waiting for execution SeqNo %d\n", n.Node.Id, acceptObj.SeqNo)
				case <-timer.C:
					txnRes.Res = false
					txnRes.Msg = "Majority Not Accepted"
					n.logger.Log("[Node %d] LEADER: Timeout waiting for execution SeqNo %d after %s\n", n.Node.Id, acceptObj.SeqNo, maxWait)
					n.pendingMutex.Lock()
					delete(n.PendingClientResponses, acceptObj.SeqNo)
					n.pendingMutex.Unlock()
					waiting = false
				}
			}
			ticker.Stop()
			timer.Stop()
		}

		n.logger.Log("[Node %d] LEADER: Executed locally SeqNo: %d, Result: (%v %v)\n", n.Node.Id, acceptObj.SeqNo, txnRes.Res, txnRes.Msg)
		n.BroadcastCommitRPC(*acceptObj)
		return &txnRes
	}

	n.logger.Log("[Node %d] MAJORITY NOT ACHIEVED: Triggering new leader election\n", n.Node.Id)
	n.logger.Log("[PAXOS] Node %d: ACCEPT failed seq=%d, forcing election\n", n.Node.Id, acceptObj.SeqNo)
	n.mu.Lock()
	n.ForceElection = true
	n.TimerRunning = true
	n.TimerExpired = false
	n.mu.Unlock()
	n.ElectionTimer.Reset(0)
	txnReply.Msg = "Majority Not Accepted"
	return &txnReply
}

func (n *NodeService) ExecuteTxn(txn configurations.Transaction, status string) (bool, string) {
	if status == "no-op" || txn.Amount == 0 {
		return true, ""
	}

	for attempt := 0; attempt < sqliteBusyRetries; attempt++ {
		tx, err := n.Node.Db.Begin()
		if err != nil {
			if isSQLiteBusy(err) {
				time.Sleep(sqliteBusyDelay)
				continue
			}
			n.logger.Log("[Node %d] DB begin failed: %v\n", n.Node.Id, err)
			return false, "DBBeginFailed"
		}

		var (
			selectStmt *sql.Stmt
			debitStmt  *sql.Stmt
			creditStmt *sql.Stmt
		)
		if n.stmtSelect != nil {
			selectStmt = tx.Stmt(n.stmtSelect)
		}
		if n.stmtDebit != nil {
			debitStmt = tx.Stmt(n.stmtDebit)
		}
		if n.stmtCredit != nil {
			creditStmt = tx.Stmt(n.stmtCredit)
		}

		closeStatements := func() {
			if selectStmt != nil {
				selectStmt.Close()
				selectStmt = nil
			}
			if debitStmt != nil {
				debitStmt.Close()
				debitStmt = nil
			}
			if creditStmt != nil {
				creditStmt.Close()
				creditStmt = nil
			}
		}

		if txn.CreditOnly {
			n.logger.Log("[Node %d] CREDIT-ONLY: key=%d amount=%d\n", n.Node.Id, txn.Receiver, txn.Amount)
			if creditStmt != nil {
				_, err = creditStmt.Exec(txn.Amount, txn.Receiver)
			} else {
				_, err = tx.Exec("UPDATE balances SET balance = balance + ? WHERE key = ?", txn.Amount, txn.Receiver)
			}
			if err != nil {
				tx.Rollback()
				if isSQLiteBusy(err) {
					time.Sleep(sqliteBusyDelay)
					closeStatements()
					continue
				}
				n.logger.Log("[Node %d] Credit-only failed: %v\n", n.Node.Id, err)
				closeStatements()
				return false, "DBWriteFailed"
			}
		} else {
			var senderBalance int
			if selectStmt != nil {
				err = selectStmt.QueryRow(txn.Sender).Scan(&senderBalance)
			} else {
				err = tx.QueryRow("SELECT balance FROM balances WHERE key = ?", txn.Sender).Scan(&senderBalance)
			}
			if err != nil {
				tx.Rollback()
				if isSQLiteBusy(err) {
					time.Sleep(sqliteBusyDelay)
					closeStatements()
					continue
				}
				n.logger.Log("[Node %d] DB read failed: %v\n", n.Node.Id, err)
				closeStatements()
				return false, "DBReadFailed"
			}
			n.logger.Log("[Node %d] Account %d has balance %d, needs %d\n", n.Node.Id, txn.Sender, senderBalance, txn.Amount)
			if senderBalance < txn.Amount {
				tx.Rollback()
				n.logger.Log("[Node %d] Insufficient funds for %d\n", n.Node.Id, txn.Sender)
				closeStatements()
				return false, "InsufficientFunds"
			}
			if debitStmt != nil {
				_, err = debitStmt.Exec(txn.Amount, txn.Sender)
			} else {
				_, err = tx.Exec("UPDATE balances SET balance = balance - ? WHERE key = ?", txn.Amount, txn.Sender)
			}
			if err != nil {
				tx.Rollback()
				if isSQLiteBusy(err) {
					time.Sleep(sqliteBusyDelay)
					closeStatements()
					continue
				}
				n.logger.Log("[Node %d] Debit failed: %v\n", n.Node.Id, err)
				closeStatements()
				return false, "DBWriteFailed"
			}
			if creditStmt != nil {
				_, err = creditStmt.Exec(txn.Amount, txn.Receiver)
			} else {
				_, err = tx.Exec("UPDATE balances SET balance = balance + ? WHERE key = ?", txn.Amount, txn.Receiver)
			}
			if err != nil {
				tx.Rollback()
				if isSQLiteBusy(err) {
					time.Sleep(sqliteBusyDelay)
					closeStatements()
					continue
				}
				n.logger.Log("[Node %d] Credit failed: %v\n", n.Node.Id, err)
				closeStatements()
				return false, "DBWriteFailed"
			}
		}
		if err := tx.Commit(); err != nil {
			tx.Rollback()
			if isSQLiteBusy(err) {
				time.Sleep(sqliteBusyDelay)
				closeStatements()
				continue
			}
			n.logger.Log("[Node %d] Commit failed: %v\n", n.Node.Id, err)
			closeStatements()
			return false, "CommitFailed"
		}

		n.modifiedKeysMutex.Lock()
		if !txn.CreditOnly {
			n.modifiedKeys[txn.Sender] = true
		}
		n.modifiedKeys[txn.Receiver] = true
		n.modifiedKeysMutex.Unlock()
		if selectStmt != nil {
			selectStmt.Close()
		}
		if debitStmt != nil {
			debitStmt.Close()
		}
		if creditStmt != nil {
			creditStmt.Close()
		}

		return true, ""
	}

	n.logger.Log("[Node %d] ExecuteTxn: exceeded retry limit for %d->%d\n", n.Node.Id, txn.Sender, txn.Receiver)
	return false, "DBBusy"
}

func (n *NodeService) handleReadOnly(txn configurations.Transaction, reply *configurations.Reply) error {
	if !n.Node.IsLive {
		reply.Msg = "Node Not Live"
		return nil
	}
	if n.hasLockConflict(txn) {
		reply.Result = false
		reply.Msg = "Locked"
		reply.Timestamp = txn.Timestamp
		n.logger.Log("[Node %d] READ-ONLY blocked: key=%d ts=%d (locked)\n", n.Node.Id, txn.Sender, txn.Timestamp)
		return nil
	}
	n.logger.Log("[Node %d] READ-ONLY: key=%d ts=%d\n", n.Node.Id, txn.Sender, txn.Timestamp)
	if balance, ok := n.getCachedBalance(txn.Sender); ok {
		reply.Result = true
		reply.Msg = fmt.Sprintf("Balance=%d", balance)
	} else {
		var balance int
		err := n.Node.Db.QueryRow("SELECT balance FROM balances WHERE key = ?", txn.Sender).Scan(&balance)
		if err != nil {
			reply.Result = false
			reply.Msg = fmt.Sprintf("ReadFailed: %v", err)
		} else {
			reply.Result = true
			reply.Msg = fmt.Sprintf("Balance=%d", balance)
			n.setCachedBalance(txn.Sender, balance)
		}
	}
	n.mu.RLock()
	reply.B = n.Node.Bnum
	n.mu.RUnlock()
	reply.Timestamp = txn.Timestamp
	return nil
}

func (n *NodeService) notifyPendingClient(seqNo int, result configurations.TxnReply) {
	n.pendingMutex.Lock()
	defer n.pendingMutex.Unlock()
	if ch, exists := n.PendingClientResponses[seqNo]; exists {
		select {
		case ch <- result:
		default:
		}
		delete(n.PendingClientResponses, seqNo)
	}
}

func (n *NodeService) ExecuteSerially(acceptObj configurations.AcceptTxn, reply *configurations.TxnReply) {
	n.executionMu.Lock()
	defer n.executionMu.Unlock()

	n.mu.RLock()
	alreadyExecuted := acceptObj.SeqNo <= n.Node.LastExecuted
	n.mu.RUnlock()
	if alreadyExecuted {
		if acceptObj.IsCrossShard && acceptObj.Role != configurations.RoleNone && acceptObj.Phase != configurations.PhasePrepare {
			n.logger.Log("[Node %d] REPLAY: SeqNo %d phase %s (cross-shard)\n", n.Node.Id, acceptObj.SeqNo, phaseLabel(acceptObj.Phase))
			txnReply := n.executeTwoPCEntry(acceptObj)
			reply.Res = txnReply.Res
			reply.Msg = txnReply.Msg
			n.resultsMutex.Lock()
			n.executionResults[acceptObj.SeqNo] = txnReply
			n.resultsMutex.Unlock()
			n.notifyPendingClient(acceptObj.SeqNo, txnReply)
			if acceptObj.Phase != configurations.PhasePrepare {
				n.releaseLocks(acceptObj.Txn)
			}
		} else {
			reply.Res = true
			reply.Msg = "DuplicateSeq"
		}
		return
	}

	n.mu.Lock()
	n.setTxnStatus(acceptObj.SeqNo, "Committed")
	if n.Node.PendingCommands == nil {
		n.Node.PendingCommands = make(map[int]configurations.AcceptTxn)
	}
	n.Node.PendingCommands[acceptObj.SeqNo] = acceptObj
	if n.WaitingRequests == nil {
		n.WaitingRequests = make(map[int]bool)
	}
	if !n.WaitingRequests[acceptObj.SeqNo] {
		n.WaitingRequests[acceptObj.SeqNo] = true
		if !n.TimerRunning {
			n.TimerRunning = true
			n.TimerExpired = false
			n.ElectionTimer.Reset(n.Node.T)
			n.logger.Log("[Node %d] BACKUP TIMER: Started for SeqNo %d (Timeout: %v)\n", n.Node.Id, acceptObj.SeqNo, n.Node.T)
		}
	}
	n.mu.Unlock()

	for {
		n.mu.Lock()
		nextSeq := n.Node.LastExecuted + 1
		if cmd, exists := n.Node.PendingCommands[nextSeq]; exists {
			dedupKey := txnDedupKey(cmd.Txn)
			execNeeded := true
			var cached configurations.Reply
			if !cmd.IsCrossShard && n.Node.TxnsProcessed != nil {
				if existing, ok := n.Node.TxnsProcessed[dedupKey]; ok && existing.Msg != "Majority Not Accepted" {
					cached = existing
					execNeeded = false
				}
			}
			if execNeeded {
				n.mu.Unlock()
				if cmd.IsCrossShard && cmd.Role != configurations.RoleNone {
					txnReply := n.executeTwoPCEntry(cmd)
					reply.Res = txnReply.Res
					reply.Msg = txnReply.Msg
				} else {
					var execMsg string
					reply.Res, execMsg = n.ExecuteTxn(cmd.Txn, cmd.Status)
					if reply.Res {
						reply.Msg = "Successful"
					} else {
						if execMsg == "" {
							execMsg = "Failed"
						}
						reply.Msg = execMsg
					}
				}
				n.mu.Lock()
				if n.Node.TxnsProcessed == nil {
					n.Node.TxnsProcessed = make(map[string]configurations.Reply)
				}
				if !cmd.IsCrossShard || cmd.Phase == configurations.PhaseCommit || cmd.Phase == configurations.PhaseAbort {
					n.Node.TxnsProcessed[dedupKey] = configurations.Reply{
						B:         cmd.B,
						Result:    reply.Res,
						Msg:       reply.Msg,
						Timestamp: cmd.Txn.Timestamp,
					}
				}
			} else {
				reply.Res = cached.Result
				reply.Msg = cached.Msg
			}
			n.resultsMutex.Lock()
			n.executionResults[nextSeq] = *reply
			n.resultsMutex.Unlock()
			n.setTxnStatus(cmd.SeqNo, "Executed")
			delete(n.Node.PendingCommands, nextSeq)
			if n.WaitingRequests != nil {
				delete(n.WaitingRequests, nextSeq)
			}
			n.Node.LastExecuted = nextSeq
			n.mu.Unlock()
			n.logger.Log("[Node %d] Transaction executed, printing DB state:\n", n.Node.Id)
			//n.PrintDB()
			n.logger.Log("[PAXOS] Node %d: EXECUTE seq=%d result=%v %s\n", n.Node.Id, nextSeq, reply.Res, reply.Msg)
			n.notifyPendingClient(nextSeq, *reply)
			releaseLocks := true
			if cmd.IsCrossShard && cmd.Role != configurations.RoleNone && cmd.Phase == configurations.PhasePrepare && reply.Res {
				releaseLocks = false
			}
			if releaseLocks {
				n.releaseLocks(cmd.Txn)
			}
			if reply.Res {
				n.invalidateTxnKeys(cmd.Txn)
			}
		} else if len(n.Node.PendingCommands) != 0 {
			reply.Msg = "Gap Found"
			n.mu.Unlock()
			break
		} else {
			n.mu.Unlock()
			break
		}
	}

	n.mu.Lock()
	pendingCount := len(n.WaitingRequests)
	if pendingCount == 0 {
		if n.TimerRunning {
			n.TimerRunning = false
			n.TimerExpired = false
			n.logger.Log("[Node %d] BACKUP TIMER: Stopped (no pending requests)\n", n.Node.Id)
		}
	} else {
		n.TimerExpired = false
		n.ElectionTimer.Reset(n.Node.T)
		n.logger.Log("[Node %d] BACKUP TIMER: Restarted (pending requests: %d, Timeout: %v)\n", n.Node.Id, pendingCount, n.Node.T)
	}
	n.mu.Unlock()
}

func (n *NodeService) executeTwoPCEntry(cmd configurations.AcceptTxn) configurations.TxnReply {
	if cmd.Role == configurations.RoleCoordinator {
		return n.executeCoordinatorEntry(cmd)
	}
	if cmd.Role == configurations.RoleParticipant {
		return n.executeParticipantEntry(cmd)
	}
	return configurations.TxnReply{Res: false, Msg: "Unknown2PCRole"}
}

func (n *NodeService) executeCoordinatorEntry(cmd configurations.AcceptTxn) configurations.TxnReply {
	state := n.getTwoPCState(cmd.TxnID)
	state.Txn = cmd.Txn
	state.Role = configurations.RoleCoordinator
	state.CoordinatorCluster = cmd.CoordinatorCluster
	state.ParticipantCluster = cmd.ParticipantCluster
	state.CoordinatorSeq = cmd.SeqNo
	reply := configurations.TxnReply{}
	n.logger.Log("[2PC] Node %d: Coordinator %s seq=%d txnID=%s\n", n.Node.Id, phaseLabel(cmd.Phase), cmd.SeqNo, cmd.TxnID)
	switch cmd.Phase {
	case configurations.PhasePrepare:
		if state.Phase == configurations.PhasePrepare || state.Phase == configurations.PhaseCommit {
			reply.Res = true
			reply.Msg = "Prepared"
			return reply
		}
		if state.Decision == configurations.PhaseAbort {
			reply.Res = false
			reply.Msg = "Aborted"
			return reply
		}
		ok, msg := n.applyCoordinatorPrepare(cmd.TxnID, cmd.Txn)
		reply.Res = ok
		reply.Msg = msg
		if ok {
			state.Phase = configurations.PhasePrepare
		} else {
			state.LastError = msg
		}
	case configurations.PhaseCommit:
		if state.Decision == configurations.PhaseCommit {
			reply.Res = true
			reply.Msg = "CommitConfirmed"
			return reply
		}
		if state.Decision == configurations.PhaseAbort {
			reply.Res = false
			reply.Msg = "Aborted"
			return reply
		}
		ok, msg := n.applyCoordinatorCommit(cmd.TxnID)
		reply.Res = ok
		reply.Msg = msg
		state.Phase = configurations.PhaseCommit
		state.Decision = configurations.PhaseCommit
		state.CoordinatorSeq = cmd.SeqNo
	case configurations.PhaseAbort:
		ok, msg := n.applyCoordinatorAbort(cmd.TxnID)
		reply.Res = ok
		reply.Msg = msg
		state.Phase = configurations.PhaseAbort
		state.Decision = configurations.PhaseAbort
		state.CoordinatorSeq = cmd.SeqNo
	default:
		reply.Res = false
		reply.Msg = "UnknownCoordinatorPhase"
	}
	return reply
}

func (n *NodeService) executeParticipantEntry(cmd configurations.AcceptTxn) configurations.TxnReply {
	state := n.getTwoPCState(cmd.TxnID)
	state.Txn = cmd.Txn
	state.Role = configurations.RoleParticipant
	state.CoordinatorCluster = cmd.CoordinatorCluster
	state.ParticipantCluster = cmd.ParticipantCluster
	state.ParticipantSeq = cmd.SeqNo
	reply := configurations.TxnReply{}
	n.logger.Log("[2PC] Node %d: Participant %s seq=%d txnID=%s\n", n.Node.Id, phaseLabel(cmd.Phase), cmd.SeqNo, cmd.TxnID)
	switch cmd.Phase {
	case configurations.PhasePrepare:
		if state.Phase == configurations.PhasePrepare {
			reply.Res = true
			reply.Msg = "Prepared"
			return reply
		}
		if state.Decision == configurations.PhaseCommit {
			reply.Res = true
			reply.Msg = "AlreadyCommitted"
			return reply
		}
		if state.Decision == configurations.PhaseAbort {
			reply.Res = false
			reply.Msg = "Aborted"
			return reply
		}
		ok, msg := n.applyParticipantPrepare(cmd.TxnID, cmd.Txn)
		reply.Res = ok
		reply.Msg = msg
		if ok {
			state.Phase = configurations.PhasePrepare
		} else {
			state.LastError = msg
		}
	case configurations.PhaseCommit:
		ok, msg := n.applyParticipantCommit(cmd.TxnID)
		reply.Res = ok
		reply.Msg = msg
		state.Phase = configurations.PhaseCommit
		state.Decision = configurations.PhaseCommit
		n.clearTwoPCState(cmd.TxnID)
		n.releaseLocks(cmd.Txn)
		if ok {
			n.invalidateTxnKeys(cmd.Txn)
		}
	case configurations.PhaseAbort:
		ok, msg := n.applyParticipantAbort(cmd.TxnID)
		reply.Res = ok
		reply.Msg = msg
		state.Phase = configurations.PhaseAbort
		state.Decision = configurations.PhaseAbort
		n.clearTwoPCState(cmd.TxnID)
		n.releaseLocks(cmd.Txn)
		if ok {
			n.invalidateTxnKeys(cmd.Txn)
		}
	default:
		reply.Res = false
		reply.Msg = "UnknownParticipantPhase"
	}
	return reply
}

func (n *NodeService) applyCoordinatorPrepare(txnID string, txn configurations.Transaction) (bool, string) {
	if n.Node.Db == nil {
		return false, "DBUnavailable"
	}
	n.logger.Log("[2PC] Node %d: Coordinator applying PREPARE txnID=%s sender=%d amt=%d\n", n.Node.Id, txnID, txn.Sender, txn.Amount)
	tx, err := n.Node.Db.Begin()
	if err != nil {
		return false, fmt.Sprintf("DBBegin:%v", err)
	}
	var balance int
	err = tx.QueryRow("SELECT balance FROM balances WHERE key = ?", txn.Sender).Scan(&balance)
	if err != nil {
		tx.Rollback()
		return false, fmt.Sprintf("Read:%v", err)
	}
	if balance < txn.Amount {
		tx.Rollback()
		return false, "InsufficientFunds"
	}
	if n.wal != nil {
		if err := n.wal.RecordTx(tx, txnID, txn.Sender, balance, configurations.PhasePrepare, configurations.RoleCoordinator); err != nil {
			tx.Rollback()
			return false, fmt.Sprintf("WAL:%v", err)
		}
	}
	if _, err := tx.Exec("UPDATE balances SET balance = balance - ? WHERE key = ?", txn.Amount, txn.Sender); err != nil {
		tx.Rollback()
		return false, fmt.Sprintf("Debit:%v", err)
	}
	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return false, fmt.Sprintf("Commit:%v", err)
	}
	n.markModifiedKey(txn.Sender)
	n.invalidateBalance(txn.Sender)
	return true, "Prepared"
}

func (n *NodeService) applyCoordinatorCommit(txnID string) (bool, string) {
	n.logger.Log("[2PC] Node %d: Coordinator finalizing COMMIT txnID=%s\n", n.Node.Id, txnID)
	if err := n.clearWal(txnID); err != nil {
		return false, fmt.Sprintf("WALClear:%v", err)
	}
	return true, "CommitConfirmed"
}

func (n *NodeService) applyCoordinatorAbort(txnID string) (bool, string) {
	n.logger.Log("[2PC] Node %d: Coordinator rolling back ABORT txnID=%s\n", n.Node.Id, txnID)
	if err := n.restoreFromWAL(txnID); err != nil {
		return false, fmt.Sprintf("Rollback:%v", err)
	}
	return true, "Aborted"
}

func (n *NodeService) applyParticipantPrepare(txnID string, txn configurations.Transaction) (bool, string) {
	if n.Node.Db == nil {
		return false, "DBUnavailable"
	}
	n.logger.Log("[2PC] Node %d: Participant applying PREPARE txnID=%s receiver=%d amt=%d\n", n.Node.Id, txnID, txn.Receiver, txn.Amount)
	tx, err := n.Node.Db.Begin()
	if err != nil {
		return false, fmt.Sprintf("DBBegin:%v", err)
	}
	var balance int
	err = tx.QueryRow("SELECT balance FROM balances WHERE key = ?", txn.Receiver).Scan(&balance)
	if err != nil {
		tx.Rollback()
		return false, fmt.Sprintf("Read:%v", err)
	}
	if n.wal != nil {
		if err := n.wal.RecordTx(tx, txnID, txn.Receiver, balance, configurations.PhasePrepare, configurations.RoleParticipant); err != nil {
			tx.Rollback()
			return false, fmt.Sprintf("WAL:%v", err)
		}
	}
	if _, err := tx.Exec("UPDATE balances SET balance = balance + ? WHERE key = ?", txn.Amount, txn.Receiver); err != nil {
		tx.Rollback()
		return false, fmt.Sprintf("Credit:%v", err)
	}
	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return false, fmt.Sprintf("Commit:%v", err)
	}
	n.markModifiedKey(txn.Receiver)
	n.invalidateBalance(txn.Receiver)
	return true, "Prepared"
}

func (n *NodeService) applyParticipantCommit(txnID string) (bool, string) {
	n.logger.Log("[2PC] Node %d: Participant finalizing COMMIT txnID=%s\n", n.Node.Id, txnID)
	if err := n.clearWal(txnID); err != nil {
		return false, fmt.Sprintf("WALClear:%v", err)
	}
	return true, "CommitConfirmed"
}

func (n *NodeService) applyParticipantAbort(txnID string) (bool, string) {
	n.logger.Log("[2PC] Node %d: Participant rolling back ABORT txnID=%s\n", n.Node.Id, txnID)
	if err := n.restoreFromWAL(txnID); err != nil {
		return false, fmt.Sprintf("Rollback:%v", err)
	}
	return true, "Aborted"
}

func (n *NodeService) restoreFromWAL(txnID string) error {
	if n.wal == nil {
		return nil
	}
	n.logger.Log("[2PC] Node %d: Restoring WAL entries for txnID=%s\n", n.Node.Id, txnID)
	images, err := n.wal.BeforeImages(txnID)
	if err != nil {
		return err
	}
	if len(images) == 0 {
		n.logger.Log("[2PC] Node %d: No WAL entries to restore for txnID=%s\n", n.Node.Id, txnID)
		return n.wal.Clear(txnID)
	}
	tx, err := n.Node.Db.Begin()
	if err != nil {
		return err
	}
	for key, before := range images {
		if _, err := tx.Exec("UPDATE balances SET balance = ? WHERE key = ?", before, key); err != nil {
			tx.Rollback()
			return err
		}
		n.markModifiedKey(key)
		n.invalidateBalance(key)
	}
	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return err
	}
	return n.wal.Clear(txnID)
}

func (n *NodeService) clearWal(txnID string) error {
	if n.wal == nil {
		return nil
	}
	return n.wal.Clear(txnID)
}

// --- RPC handlers ---

func (n *NodeService) Commit(acceptObj configurations.AcceptTxn, reply *configurations.TxnReply) error {
	if !n.Node.IsLive {
		return nil
	}
	n.ExecuteSerially(acceptObj, reply)
	n.logger.Log("[Node %d] COMMIT: Txn (%v -> %v amount %v) SeqNo %d committed\n", n.Node.Id, acceptObj.Txn.Sender, acceptObj.Txn.Receiver, acceptObj.Txn.Amount, acceptObj.SeqNo)
	return nil
}

func (n *NodeService) ClientRequest(txn configurations.Transaction, reply *configurations.Reply) error {
	if !n.Node.IsLive {
		reply.Msg = "Node Not Live"
		return nil
	}
	if txn.ReadOnly {
		n.logger.Log("[PAXOS] Node %d: CLIENT READ txn %d ts=%d\n", n.Node.Id, txn.Sender, txn.Timestamp)
		return n.handleReadOnly(txn, reply)
	}
	n.logger.Log("[PAXOS] Node %d: CLIENT txn %d->%d amt=%d ts=%d\n", n.Node.Id, txn.Sender, txn.Receiver, txn.Amount, txn.Timestamp)

	// Basic deduplication on TxnID
	n.mu.Lock()
	if n.Node.ClientLastReply == nil {
		n.Node.ClientLastReply = make(map[int]int)
	}
	if n.Node.TxnsProcessed == nil {
		n.Node.TxnsProcessed = make(map[string]configurations.Reply)
	}
	dedupKey := txnDedupKey(txn)
	if val, ok := n.Node.TxnsProcessed[dedupKey]; ok && val.Msg != "Majority Not Accepted" {
		if val.Result || !isTransientReply(val.Msg) {
			*reply = val
			n.mu.Unlock()
			return nil
		}
		delete(n.Node.TxnsProcessed, dedupKey)
	}
	n.mu.Unlock()

	// Lock acquisition before consensus
	if !n.acquireLocks(txn) {
		reply.Msg = "Locked"
		reply.Result = false
		return nil
	}

	// Leader only
	if n.Node.Bnum.NodeID == 0 {
		n.logger.Log("[Node %d] CLIENT txn %d->%d refused: leader unknown, forcing election\n", n.Node.Id, txn.Sender, txn.Receiver)
		n.mu.Lock()
		n.ForceElection = true
		n.mu.Unlock()
		n.ElectionTimer.Reset(0)
		reply.Msg = "LeaderUnknown"
		reply.Result = false
		reply.Timestamp = txn.Timestamp
		n.releaseLocks(txn)
		return nil
	}
	if n.Node.Id != n.Node.Bnum.NodeID {
		n.logger.Log("[Node %d] CLIENT txn %d->%d refused: not leader (leader=%d)\n", n.Node.Id, txn.Sender, txn.Receiver, n.Node.Bnum.NodeID)
		n.mu.Lock()
		n.ForceElection = true
		n.mu.Unlock()
		n.ElectionTimer.Reset(0)
		reply.B = n.Node.Bnum
		reply.Result = false
		reply.Msg = "NotLeader"
		reply.Timestamp = txn.Timestamp
		n.releaseLocks(txn)
		return nil
	}

	participantCluster := configurations.ResolveClusterIDForKey(txn.Receiver)
	if participantCluster != 0 && participantCluster != n.Node.ClusterId {
		err := n.handleCrossShardCoordinator(txn, participantCluster, dedupKey, reply)
		return err
	}

	if !n.isNewView && n.Node.Bnum.NodeID == n.Node.Id {
		n.logger.Log("[Node %d] LEADER: Processing client request\n", n.Node.Id)
		txnRes := n.BroadcastAcceptRPC(txn)
		n.mu.Lock()
		reply.B.B = n.Node.Bnum.B
		reply.B.NodeID = n.Node.Id
		reply.Result = txnRes.Res
		reply.Timestamp = txn.Timestamp
		reply.Msg = txnRes.Msg
		n.Node.ClientLastReply[txn.Sender] = txn.Timestamp
		n.Node.TxnsProcessed[dedupKey] = *reply
		n.mu.Unlock()
		n.releaseLocks(txn)
	} else {
		n.logger.Log("[Node %d] Not leader/in NEW-VIEW, triggering election\n", n.Node.Id)
		n.mu.Lock()
		n.ForceElection = true
		n.mu.Unlock()
		n.ElectionTimer.Reset(0)
		reply.Msg = "Majority Not Accepted"
		n.releaseLocks(txn)
	}
	return nil
}

func (n *NodeService) handleCrossShardCoordinator(txn configurations.Transaction, participantCluster int, dedupKey string, reply *configurations.Reply) error {
	txnID := txnIdentity(txn)
	reply.B.B = n.Node.Bnum.B
	reply.B.NodeID = n.Node.Id
	req := commitment.PrepareRequest{
		Txn:                txn,
		TxnID:              txnID,
		CoordinatorID:      n.Node.Id,
		CoordinatorCluster: n.Node.ClusterId,
		ParticipantCluster: participantCluster,
		Timestamp:          txn.Timestamp,
	}
	n.logger.Log("[2PC] Node %d: Coordinator sending PREPARE to cluster %d txnID=%s\n", n.Node.Id, participantCluster, txnID)
	var prepareResp *commitment.PrepareResponse
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		prepareResp, err = n.sendParticipantPrepare(req)
		if err == nil {
			break
		}
		n.logger.Log("[2PC] Node %d: Participant prepare attempt %d failed: %v\n", n.Node.Id, attempt+1, err)
	}
	if err != nil {
		n.logger.Log("[2PC] Node %d: Participant prepare failed: %v\n", n.Node.Id, err)
		reply.Result = false
		reply.Msg = fmt.Sprintf("ParticipantPrepare:%v", err)
		reply.Timestamp = txn.Timestamp
		n.releaseLocks(txn)
		n.recordClientReply(dedupKey, txn, reply)
		return nil
	}
	if prepareResp.LeaderID != 0 {
		n.logger.Log("[2PC] Node %d: Participant leader identified as n%d\n", n.Node.Id, prepareResp.LeaderID)
	}
	if !prepareResp.Ready {
		msg := prepareResp.Reason
		if msg == "" {
			msg = "ParticipantRejected"
		}
		n.logger.Log("[2PC] Node %d: Participant declined prepare: %s\n", n.Node.Id, msg)
		reply.Result = false
		reply.Msg = msg
		reply.Timestamp = txn.Timestamp
		n.releaseLocks(txn)
		n.recordClientReply(dedupKey, txn, reply)
		return nil
	}
	state := n.getTwoPCState(txnID)
	state.ParticipantSeq = prepareResp.ParticipantSeq
	state.ParticipantLeaderID = prepareResp.LeaderID
	state.Txn = txn
	state.Role = configurations.RoleCoordinator
	state.CoordinatorCluster = n.Node.ClusterId
	state.ParticipantCluster = participantCluster
	n.logger.Log("[2PC] Node %d: Participant prepared txnID=%s participantSeq=%d\n", n.Node.Id, txnID, prepareResp.ParticipantSeq)
	prepareEntry := configurations.AcceptTxn{
		Txn:                txn,
		Phase:              configurations.PhasePrepare,
		Role:               configurations.RoleCoordinator,
		TxnID:              txnID,
		Status:             "2pc-coordinator",
		IsCrossShard:       true,
		CoordinatorCluster: n.Node.ClusterId,
		ParticipantCluster: participantCluster,
	}
	prepRes := n.broadcastCustomAccept(&prepareEntry)
	if prepRes == nil || !prepRes.Res {
		n.logger.Log("[2PC] Node %d: Coordinator prepare entry failed for txn %s: %v\n", n.Node.Id, txnID, prepRes)
		if err := n.sendParticipantDecision(commitment.DecisionRequest{
			TxnID:              txnID,
			Decision:           configurations.PhaseAbort,
			CoordinatorSeq:     prepareEntry.SeqNo,
			ParticipantSeq:     prepareResp.ParticipantSeq,
			CoordinatorCluster: n.Node.ClusterId,
			ParticipantCluster: participantCluster,
			Txn:                txn,
		}); err != nil {
			n.logger.Log("[2PC] Node %d: Abort notification failed txnID=%s err=%v\n", n.Node.Id, txnID, err)
		}
		reply.Result = false
		if state.LastError != "" {
			reply.Msg = state.LastError
		} else if prepRes != nil && prepRes.Msg != "" {
			reply.Msg = prepRes.Msg
		} else {
			reply.Msg = "PrepareFailed"
		}
		reply.Timestamp = txn.Timestamp
		n.releaseLocks(txn)
		n.recordClientReply(dedupKey, txn, reply)
		return nil
	}
	coordinatorSeq := prepareEntry.SeqNo

	decision := configurations.PhaseCommit
	decisionSeq := coordinatorSeq
	commitEntry := configurations.AcceptTxn{
		Txn:                txn,
		Phase:              configurations.PhaseCommit,
		Role:               configurations.RoleCoordinator,
		TxnID:              txnID,
		Status:             "2pc-coordinator",
		IsCrossShard:       true,
		CoordinatorCluster: n.Node.ClusterId,
		ParticipantCluster: participantCluster,
		SeqNo:              coordinatorSeq,
	}
	commitRes := n.broadcastCustomAccept(&commitEntry)
	if commitRes == nil || !commitRes.Res {
		n.logger.Log("[2PC] Node %d: Commit entry failed, issuing abort txn %s\n", n.Node.Id, txnID)
		decision = configurations.PhaseAbort
		abortEntry := configurations.AcceptTxn{
			Txn:                txn,
			Phase:              configurations.PhaseAbort,
			Role:               configurations.RoleCoordinator,
			TxnID:              txnID,
			Status:             "2pc-coordinator",
			IsCrossShard:       true,
			CoordinatorCluster: n.Node.ClusterId,
			ParticipantCluster: participantCluster,
			SeqNo:              coordinatorSeq,
		}
		if abortRes := n.broadcastCustomAccept(&abortEntry); abortRes == nil || !abortRes.Res {
			n.logger.Log("[2PC] Node %d: Abort entry failed for txn %s\n", n.Node.Id, txnID)
			n.releaseLocks(txn)
		} else {
			decisionSeq = abortEntry.SeqNo
		}
	}
	decisionReq := commitment.DecisionRequest{
		TxnID:              txnID,
		Decision:           decision,
		CoordinatorSeq:     decisionSeq,
		ParticipantSeq:     prepareResp.ParticipantSeq,
		CoordinatorCluster: n.Node.ClusterId,
		ParticipantCluster: participantCluster,
		Txn:                txn,
	}
	state.Decision = decision
	if err := n.sendParticipantDecision(decisionReq); err != nil {
		n.logger.Log("[2PC] Node %d: Decision delivery failed txnID=%s err=%v\n", n.Node.Id, txnID, err)
	} else {
		n.clearTwoPCState(txnID)
	}

	reply.Timestamp = txn.Timestamp
	reply.B.B = n.Node.Bnum.B
	reply.B.NodeID = n.Node.Id
	if decision == configurations.PhaseCommit && commitRes != nil && commitRes.Res {
		reply.Result = true
		reply.Msg = "Successful"
	} else {
		reply.Result = false
		reply.Msg = "Aborted"
	}
	n.releaseLocks(txn)
	n.recordClientReply(dedupKey, txn, reply)
	return nil
}

func (n *NodeService) recordClientReply(dedupKey string, txn configurations.Transaction, reply *configurations.Reply) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.Node.TxnsProcessed == nil {
		n.Node.TxnsProcessed = make(map[string]configurations.Reply)
	}
	if n.Node.ClientLastReply == nil {
		n.Node.ClientLastReply = make(map[int]int)
	}
	n.Node.TxnsProcessed[dedupKey] = *reply
	n.Node.ClientLastReply[txn.Sender] = txn.Timestamp
}

func (n *NodeService) sendParticipantPrepare(req commitment.PrepareRequest) (*commitment.PrepareResponse, error) {
	nodeIDs := configurations.GetClusterNodeIDs(req.ParticipantCluster)
	if len(nodeIDs) == 0 {
		return nil, fmt.Errorf("no participants")
	}
	var lastErr error
	targetIdx := 0
	target := nodeIDs[targetIdx%len(nodeIDs)]
	for attempts := 0; attempts < len(nodeIDs)*2; attempts++ {
		n.logger.Log("[2PC] Node %d: PREPARE RPC attempt %d to n%d for txnID=%s\n", n.Node.Id, attempts+1, target, req.TxnID)
		var resp commitment.PrepareResponse
		err := n.callPrepareRPC(target, req, &resp)
		if err != nil {
			lastErr = err
			targetIdx++
			target = nodeIDs[targetIdx%len(nodeIDs)]
			continue
		}
		if resp.LeaderID != 0 && resp.LeaderID != target {
			n.logger.Log("[2PC] Node %d: PREPARE redirected to participant leader n%d\n", n.Node.Id, resp.LeaderID)
			target = resp.LeaderID
			continue
		}
		n.logger.Log("[2PC] Node %d: PREPARE RPC success from n%d ready=%t reason=%s\n", n.Node.Id, target, resp.Ready, resp.Reason)
		if !resp.Ready && (resp.Reason == "NodeNotLive" || resp.Reason == "LeaderUnknown" || resp.Reason == "NotLeader") {
			lastErr = fmt.Errorf(resp.Reason)
			targetIdx++
			target = nodeIDs[targetIdx%len(nodeIDs)]
			continue
		}
		return &resp, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("participant prepare failed")
	}
	return nil, lastErr
}

func (n *NodeService) sendParticipantDecision(req commitment.DecisionRequest) error {
	nodeIDs := configurations.GetClusterNodeIDs(req.ParticipantCluster)
	if len(nodeIDs) == 0 {
		return fmt.Errorf("no participants")
	}
	targetIdx := 0
	target := nodeIDs[targetIdx]
	attempt := 0
	for {
		attempt++
		n.logger.Log("[2PC] Node %d: DECISION attempt %d to n%d txnID=%s decision=%s\n", n.Node.Id, attempt, target, req.TxnID, phaseLabel(req.Decision))
		var resp commitment.DecisionResponse
		err := n.callDecisionRPC(target, req, &resp)
		if err != nil {
			n.logger.Log("[2PC] Node %d: DECISION RPC to n%d failed: %v\n", n.Node.Id, target, err)
			targetIdx = (targetIdx + 1) % len(nodeIDs)
			target = nodeIDs[targetIdx]
			time.Sleep(n.participantRetryDelay())
			continue
		}
		if resp.LeaderID != 0 && resp.LeaderID != target {
			n.logger.Log("[2PC] Node %d: DECISION redirected to leader n%d\n", n.Node.Id, resp.LeaderID)
			target = resp.LeaderID
			continue
		}
		if resp.Ack {
			n.logger.Log("[2PC] Node %d: Participant n%d acked decision %s\n", n.Node.Id, target, phaseLabel(req.Decision))
			return nil
		}
		if resp.Reason == "NotLeader" || resp.Reason == "NodeNotLive" || resp.Reason == "LeaderUnknown" {
			if resp.LeaderID != 0 && resp.LeaderID != target {
				target = resp.LeaderID
			} else {
				targetIdx = (targetIdx + 1) % len(nodeIDs)
				target = nodeIDs[targetIdx]
			}
			time.Sleep(n.participantRetryDelay())
			continue
		}
		return fmt.Errorf(resp.Reason)
	}
}

func (n *NodeService) callPrepareRPC(nodeID int, req commitment.PrepareRequest, resp *commitment.PrepareResponse) error {
	return n.callPeerRPC(nodeID, "Node.Prepare2PC", req, resp)
}

func (n *NodeService) callDecisionRPC(nodeID int, req commitment.DecisionRequest, resp *commitment.DecisionResponse) error {
	return n.callPeerRPC(nodeID, "Node.Decision2PC", req, resp)
}

func (n *NodeService) Prepare2PC(req commitment.PrepareRequest, resp *commitment.PrepareResponse) error {
	resp.TxnID = req.TxnID
	n.logger.Log("[2PC] Node %d: PREPARE RPC txnID=%s\n", n.Node.Id, req.TxnID)
	if !n.Node.IsLive {
		resp.Ready = false
		resp.Reason = "NodeNotLive"
		return nil
	}
	if n.Node.Bnum.NodeID == 0 || n.Node.Bnum.NodeID != n.Node.Id {
		resp.Ready = false
		resp.LeaderID = n.Node.Bnum.NodeID
		resp.Reason = "NotLeader"
		return nil
	}
	if existing, ok := n.loadTwoPCState(req.TxnID); ok {
		if existing.Phase == configurations.PhasePrepare && existing.Decision == configurations.PhaseNone {
			resp.Ready = true
			resp.ParticipantSeq = existing.ParticipantSeq
			resp.ParticipantPhase = configurations.PhasePrepare
			resp.LeaderID = existing.ParticipantLeaderID
			n.logger.Log("[2PC] Node %d: PREPARE duplicate acknowledged txnID=%s seq=%d\n", n.Node.Id, req.TxnID, existing.ParticipantSeq)
			return nil
		}
		if existing.Decision == configurations.PhaseCommit {
			resp.Ready = false
			resp.Reason = "AlreadyCommitted"
			return nil
		}
		if existing.Decision == configurations.PhaseAbort {
			resp.Ready = false
			resp.Reason = "AlreadyAborted"
			return nil
		}
	}
	if !n.acquireLocks(req.Txn) {
		resp.Ready = false
		resp.Reason = "Locked"
		return nil
	}
	acceptObj := configurations.AcceptTxn{
		Txn:                req.Txn,
		Phase:              configurations.PhasePrepare,
		Role:               configurations.RoleParticipant,
		TxnID:              req.TxnID,
		Status:             "2pc-participant",
		IsCrossShard:       true,
		CoordinatorCluster: req.CoordinatorCluster,
		ParticipantCluster: req.ParticipantCluster,
	}
	txnRes := n.broadcastCustomAccept(&acceptObj)
	resp.ParticipantSeq = acceptObj.SeqNo
	resp.LeaderID = n.Node.Id
	if txnRes == nil || !txnRes.Res {
		resp.Ready = false
		if txnRes != nil && txnRes.Msg != "" {
			resp.Reason = txnRes.Msg
		} else {
			resp.Reason = "PrepareFailed"
		}
		n.releaseLocks(req.Txn)
		return nil
	}
	resp.Ready = true
	resp.ParticipantPhase = configurations.PhasePrepare
	state := n.getTwoPCState(req.TxnID)
	state.Txn = req.Txn
	state.Role = configurations.RoleParticipant
	state.CoordinatorCluster = req.CoordinatorCluster
	state.ParticipantCluster = req.ParticipantCluster
	state.ParticipantSeq = acceptObj.SeqNo
	state.ParticipantLeaderID = n.Node.Id
	n.startParticipantDecisionTimer(req.TxnID, req.Txn)
	n.logger.Log("[2PC] Node %d: PREPARE completed txnID=%s seq=%d\n", n.Node.Id, req.TxnID, acceptObj.SeqNo)
	return nil
}

func (n *NodeService) Decision2PC(req commitment.DecisionRequest, resp *commitment.DecisionResponse) error {
	resp.TxnID = req.TxnID
	n.logger.Log("[2PC] Node %d: DECISION RPC txnID=%s decision=%s\n", n.Node.Id, req.TxnID, phaseLabel(req.Decision))
	if !n.Node.IsLive {
		resp.Ack = false
		resp.Reason = "NodeNotLive"
		resp.LeaderID = 0
		return nil
	}
	if n.Node.Bnum.NodeID == 0 || n.Node.Bnum.NodeID != n.Node.Id {
		resp.Ack = false
		resp.Reason = "NotLeader"
		resp.LeaderID = n.Node.Bnum.NodeID
		return nil
	}
	if existing, ok := n.loadTwoPCState(req.TxnID); ok {
		if existing.Decision == req.Decision && existing.AcknowledgedDecision {
			resp.Ack = true
			resp.LeaderID = n.Node.Id
			return nil
		}
		if existing.Decision != configurations.PhaseNone && existing.Decision != req.Decision {
			resp.Ack = false
			resp.Reason = "DecisionMismatch"
			resp.LeaderID = n.Node.Id
			return nil
		}
	}
	acceptObj := configurations.AcceptTxn{
		Txn:                req.Txn,
		Phase:              req.Decision,
		Role:               configurations.RoleParticipant,
		TxnID:              req.TxnID,
		Status:             "2pc-participant",
		IsCrossShard:       true,
		CoordinatorCluster: req.CoordinatorCluster,
		ParticipantCluster: req.ParticipantCluster,
	}
	if req.ParticipantSeq > 0 {
		acceptObj.SeqNo = req.ParticipantSeq
	}
	txnRes := n.broadcastCustomAccept(&acceptObj)
	if txnRes == nil || !txnRes.Res {
		resp.Ack = false
		if txnRes != nil && txnRes.Msg != "" {
			resp.Reason = txnRes.Msg
		} else {
			resp.Reason = "DecisionFailed"
		}
		resp.LeaderID = n.Node.Id
		return nil
	}
	resp.Ack = true
	n.logger.Log("[2PC] Node %d: DECISION applied txnID=%s decision=%s\n", n.Node.Id, req.TxnID, phaseLabel(req.Decision))
	resp.LeaderID = n.Node.Id
	return nil
}

func (n *NodeService) QueryTwoPCState(req commitment.TwoPCStatusRequest, resp *commitment.TwoPCStatusResponse) error {
	status, ok := n.buildTwoPCStatus(req.TxnID)
	status.Known = ok
	*resp = status
	return nil
}

func (n *NodeService) Accept(accpetObj configurations.AcceptTxn, reply *configurations.AcceptTxn) error {
	if !n.Node.IsLive {
		return nil
	}
	n.mu.Lock()
	defer n.mu.Unlock()

	n.logger.Log("[Node %d] ACCEPT RPC: From Node %d | SeqNo: %d | Ballot (%v) Vs (%v)\n", n.Node.Id, accpetObj.B.NodeID, accpetObj.SeqNo, n.Node.Bnum, accpetObj.B)
	n.logger.Log("[PAXOS] Node %d: ACCEPT recv seq=%d from n%d ballot=%v\n", n.Node.Id, accpetObj.SeqNo, accpetObj.B.NodeID, accpetObj.B)

	if n.Node.Bnum.B <= accpetObj.B.B {
		n.Node.Bnum.B = accpetObj.B.B
		n.Node.Bnum.NodeID = accpetObj.B.NodeID
		reply.Acceptance = 1
		reply.B = accpetObj.B
		reply.SeqNo = accpetObj.SeqNo
		reply.Txn = accpetObj.Txn

		var acceptEntry configurations.AcceptLog
		acceptEntry.AcceptNum = accpetObj.B
		acceptEntry.AcceptSeq = accpetObj.SeqNo
		acceptEntry.AcceptVal = accpetObj.Txn
		acceptEntry.Status = accpetObj.Status
		acceptEntry.Phase = accpetObj.Phase
		acceptEntry.Role = accpetObj.Role
		acceptEntry.TxnID = accpetObj.TxnID
		acceptEntry.IsCrossShard = accpetObj.IsCrossShard
		acceptEntry.CoordinatorCluster = accpetObj.CoordinatorCluster
		acceptEntry.ParticipantCluster = accpetObj.ParticipantCluster

		if n.Node.SequenceNumber < accpetObj.SeqNo {
			n.Node.SequenceNumber = accpetObj.SeqNo
		}
		n.Node.AcceptLog = append(n.Node.AcceptLog, acceptEntry)
		n.ensureTxnStatus(accpetObj.SeqNo, "Accepted")
		if n.WaitingRequests == nil {
			n.WaitingRequests = make(map[int]bool)
		}
		if !n.WaitingRequests[accpetObj.SeqNo] {
			n.WaitingRequests[accpetObj.SeqNo] = true
			if !n.TimerRunning {
				n.TimerRunning = true
				n.TimerExpired = false
				n.ElectionTimer.Reset(n.Node.T)
				n.logger.Log("[Node %d] BACKUP TIMER: Started for SeqNo %d (Timeout: %v)\n", n.Node.Id, accpetObj.SeqNo, n.Node.T)
			}
		}
	} else {
		reply.Acceptance = 0
		n.logger.Log("[Node %d] ACCEPT: Rejected (Ballot too low: %v)\n", n.Node.Id, accpetObj.B)
	}
	return nil
}

func (n *NodeService) Prepare(ballotNum configurations.BallotNumber, promise *configurations.Promise) error {
	if !n.Node.IsLive {
		return nil
	}
	n.TpTimer.Reset(n.Node.Tp)
	n.mu.Lock()
	defer n.mu.Unlock()

	n.logger.Log("[Node %d] PREPARE RPC: From Node %d | Ballot (%v) Vs (%v)\n", n.Node.Id, ballotNum.NodeID, n.Node.Bnum, ballotNum)
	n.logger.Log("[PAXOS] Node %d: PREPARE recv from n%d ballot=%v\n", n.Node.Id, ballotNum.NodeID, ballotNum)

	if n.Node.Bnum.B < ballotNum.B {
		n.Node.Bnum.B = ballotNum.B
		n.Node.Bnum.NodeID = ballotNum.NodeID
		promise.AcceptedMsgs = n.Node.AcceptLog
		promise.Vote = 1
		n.logger.Log("[Node %d] PREPARE: Promised to Node %d\n", n.Node.Id, ballotNum.NodeID)
	} else {
		promise.AcceptedMsgs = nil
		promise.Vote = 0
		n.logger.Log("[Node %d] PREPARE: Rejected Node %d (Ballot too low: %v)\n", n.Node.Id, ballotNum.NodeID, ballotNum)
	}
	return nil
}

func (n *NodeService) NewView(newViewObj configurations.NewViewInput, acceptanceMap *map[int]configurations.AcceptTxn) error {
	if !n.Node.IsLive {
		return nil
	}
	n.recordNewViewMessage(newViewObj.B, newViewObj.AcceptLog)
	n.mu.Lock()
	defer n.mu.Unlock()
	n.logger.Log("[Node %d] NEW VIEW: Processing new view (entries: %d)\n", n.Node.Id, len(newViewObj.AcceptLog))
	n.logger.Log("[PAXOS] Node %d: NEW VIEW received from ballot=%v entries=%d\n", n.Node.Id, newViewObj.B, len(newViewObj.AcceptLog))
	acceptLog := newViewObj.AcceptLog
	maxSeq := -1
	for _, entry := range acceptLog {
		if n.Node.Bnum.B <= entry.AcceptNum.B {
			n.Node.Bnum.B = entry.AcceptNum.B
			n.Node.Bnum.NodeID = entry.AcceptNum.NodeID

			var acceptEntry configurations.AcceptLog
			acceptEntry.AcceptNum = entry.AcceptNum
			acceptEntry.AcceptSeq = entry.AcceptSeq
			acceptEntry.AcceptVal = entry.AcceptVal
			acceptEntry.Status = entry.Status
			acceptEntry.Phase = entry.Phase
			acceptEntry.Role = entry.Role
			acceptEntry.TxnID = entry.TxnID
			acceptEntry.IsCrossShard = entry.IsCrossShard
			acceptEntry.CoordinatorCluster = entry.CoordinatorCluster
			acceptEntry.ParticipantCluster = entry.ParticipantCluster
			n.Node.AcceptLog = append(n.Node.AcceptLog, acceptEntry)

			(*acceptanceMap)[entry.AcceptSeq] = configurations.AcceptTxn{
				B:                  acceptEntry.AcceptNum,
				Txn:                acceptEntry.AcceptVal,
				SeqNo:              acceptEntry.AcceptSeq,
				Acceptance:         1,
				Status:             acceptEntry.Status,
				Phase:              acceptEntry.Phase,
				Role:               acceptEntry.Role,
				TxnID:              acceptEntry.TxnID,
				IsCrossShard:       acceptEntry.IsCrossShard,
				CoordinatorCluster: acceptEntry.CoordinatorCluster,
				ParticipantCluster: acceptEntry.ParticipantCluster,
			}
			if entry.AcceptSeq > maxSeq {
				maxSeq = entry.AcceptSeq
			}
		} else {
			(*acceptanceMap)[entry.AcceptSeq] = configurations.AcceptTxn{}
		}
	}
	if maxSeq >= 0 && n.Node.SequenceNumber < maxSeq {
		n.Node.SequenceNumber = maxSeq
	}
	return nil
}

func (n *NodeService) FailNode(isLive bool, reply *bool) error {
	n.StartupPhase = false
	if isLive {
		n.mu.Lock()
		if n.Node.IsLive {
			n.mu.Unlock()
			n.logger.Log("[Node %d] STATUS: Node already LIVE\n", n.Node.Id)
			*reply = true
			return nil
		}
		n.logger.Log("[Node %d] STATUS: Node RECOVERING\n", n.Node.Id)
		n.mu.Unlock()
		n.catchUpState()
		n.mu.Lock()
		n.Node.IsLive = true
		n.mu.Unlock()
		n.resumeTwoPCTimers()
		n.logger.Log("[Node %d] STATUS: Node LIVE\n", n.Node.Id)
	} else {
		n.mu.Lock()
		n.Node.IsLive = false
		n.mu.Unlock()
		n.stopAllTwoPCTimers()
		n.logger.Log("[Node %d] STATUS: Node FAILED\n", n.Node.Id)
	}
	*reply = true
	return nil
}

func (n *NodeService) SetBenchmarkMode(enabled bool, reply *bool) error {
	n.mu.Lock()
	n.benchmarkMode = enabled
	n.mu.Unlock()
	if n.logger != nil {
		if enabled {
			n.logger.Clear()
		}
		n.logger.SetMuted(enabled)
	}
	if reply != nil {
		*reply = true
	}
	n.logger.Log("[Node %d] STATUS: Benchmark mode set to %t\n", n.Node.Id, enabled)
	return nil
}

func (n *NodeService) catchUpState() {
	peers := configurations.GetClusterNodeIDs(n.Node.ClusterId)
	var best configurations.StateSnapshot
	found := false
	n.mu.RLock()
	currentLast := n.Node.LastExecuted
	n.mu.RUnlock()
	for _, peer := range peers {
		if peer == n.Node.Id {
			continue
		}
		var snapshot configurations.StateSnapshot
		if err := n.callPeerRPC(peer, "Node.GetStateSnapshot", true, &snapshot); err != nil {
			n.logger.Log("[Node %d] CATCH-UP: snapshot RPC to n%d failed: %v\n", n.Node.Id, peer, err)
			continue
		}
		if !found || snapshot.LastExecuted > best.LastExecuted || (snapshot.LastExecuted == best.LastExecuted && len(snapshot.AcceptLog) > len(best.AcceptLog)) {
			best = snapshot
			found = true
		}
	}
	if !found {
		n.logger.Log("[Node %d] CATCH-UP: no peer state available\n", n.Node.Id)
		return
	}

	maxSeq := 0
	highestBallot := configurations.BallotNumber{}
	for _, entry := range best.AcceptLog {
		if entry.AcceptSeq > maxSeq {
			maxSeq = entry.AcceptSeq
		}
		if entry.AcceptNum.B > highestBallot.B || (entry.AcceptNum.B == highestBallot.B && entry.AcceptNum.NodeID > highestBallot.NodeID) {
			highestBallot = entry.AcceptNum
		}
	}

	n.mu.Lock()
	n.Node.AcceptLog = make([]configurations.AcceptLog, len(best.AcceptLog))
	copy(n.Node.AcceptLog, best.AcceptLog)
	if maxSeq > n.Node.SequenceNumber {
		n.Node.SequenceNumber = maxSeq
	}
	n.mu.Unlock()

	if best.LastExecuted <= currentLast {
		n.logger.Log("[Node %d] CATCH-UP: no newer state available (last=%d)\n", n.Node.Id, currentLast)
	} else {
		n.logger.Log("[Node %d] CATCH-UP: applying entries up to seq %d (current=%d)\n", n.Node.Id, best.LastExecuted, currentLast)
		for _, entry := range best.AcceptLog {
			if entry.AcceptSeq > best.LastExecuted {
				continue
			}
			n.mu.RLock()
			localLast := n.Node.LastExecuted
			n.mu.RUnlock()
			replayNeeded := entry.AcceptSeq > localLast
			if !replayNeeded && entry.IsCrossShard && entry.Role != configurations.RoleNone && entry.Phase != configurations.PhasePrepare {
				replayNeeded = true
			}
			if !replayNeeded {
				continue
			}
			acceptObj := configurations.AcceptTxn{
				B:                  entry.AcceptNum,
				Txn:                entry.AcceptVal,
				SeqNo:              entry.AcceptSeq,
				Acceptance:         1,
				Status:             entry.Status,
				Phase:              entry.Phase,
				Role:               entry.Role,
				TxnID:              entry.TxnID,
				IsCrossShard:       entry.IsCrossShard,
				CoordinatorCluster: entry.CoordinatorCluster,
				ParticipantCluster: entry.ParticipantCluster,
			}
			var reply configurations.TxnReply
			n.ExecuteSerially(acceptObj, &reply)
			n.logger.Log("[Node %d] CATCH-UP: applied seq %d (res=%v %s)\n", n.Node.Id, acceptObj.SeqNo, reply.Res, reply.Msg)
		}
	}

	n.mu.Lock()
	currentBallot := n.Node.Bnum
	if highestBallot.B > currentBallot.B || (highestBallot.B == currentBallot.B && highestBallot.NodeID > currentBallot.NodeID) {
		currentBallot = highestBallot
	}
	if currentBallot.B == 0 {
		currentBallot.B = 1
		if currentBallot.NodeID == 0 {
			currentBallot.NodeID = configurations.GetClusterLeaderID(n.Node.ClusterId)
		}
	}
	n.Node.Bnum = currentBallot
	n.mu.Unlock()
}

func (n *NodeService) FlushState(_ bool, reply *bool) error {
	n.mu.Lock()
	n.Node.AcceptLog = nil
	n.Node.SequenceNumber = 0
	n.Node.LastExecuted = 0
	n.Node.PendingCommands = make(map[int]configurations.AcceptTxn)
	n.Node.ClientLastReply = make(map[int]int)
	n.Node.TxnsProcessed = make(map[string]configurations.Reply)
	n.Node.Bnum.B = 1
	leaderID := configurations.GetClusterLeaderID(n.Node.ClusterId)
	n.Node.Bnum.NodeID = leaderID
	n.TimerRunning = false
	n.TimerExpired = false
	n.WaitingRequests = make(map[int]bool)
	n.resetTxnStatus()
	n.mu.Unlock()
	n.resultsMutex.Lock()
	n.executionResults = make(map[int]configurations.TxnReply)
	n.resultsMutex.Unlock()
	n.pendingMutex.Lock()
	n.PendingClientResponses = make(map[int]chan configurations.TxnReply)
	n.pendingMutex.Unlock()
	n.modifiedKeysMutex.Lock()
	modKeys := make([]int, 0, len(n.modifiedKeys))
	for k := range n.modifiedKeys {
		modKeys = append(modKeys, k)
	}
	n.modifiedKeys = make(map[int]bool)
	n.modifiedKeysMutex.Unlock()
	n.cacheMu.Lock()
	n.balanceCache = make(map[int]int)
	n.cacheMu.Unlock()
	n.locks = make(map[int]string)
	n.resetPeerClientPool()
	if n.ElectionTimer != nil {
		if !n.ElectionTimer.Stop() {
			select {
			case <-n.ElectionTimer.C:
			default:
			}
		}
		n.ElectionTimer.Reset(n.Node.T)
	}
	if n.TpTimer != nil {
		if !n.TpTimer.Stop() {
			select {
			case <-n.TpTimer.C:
			default:
			}
		}
		n.TpTimer.Reset(n.Node.Tp)
	}
	for _, key := range modKeys {
		n.Node.Db.Exec("UPDATE balances SET balance = 10 WHERE key = ?", key)
	}
	n.newViewMu.Lock()
	n.newViewMessages = nil
	n.newViewMu.Unlock()
	if n.wal != nil {
		n.wal.ClearAll()
	}
	n.twoPCMu.Lock()
	n.twoPCState = make(map[string]*commitment.TransactionState)
	n.twoPCMu.Unlock()
	n.logger.Log("[Node %d] State flushed (reset %d keys)\n", n.Node.Id, len(modKeys))
	*reply = true
	return nil
}

func (n *NodeService) GetStateSnapshot(_ bool, snapshot *configurations.StateSnapshot) error {
	n.mu.RLock()
	defer n.mu.RUnlock()
	snapshot.LastExecuted = n.Node.LastExecuted
	snapshot.AcceptLog = make([]configurations.AcceptLog, len(n.Node.AcceptLog))
	copy(snapshot.AcceptLog, n.Node.AcceptLog)
	return nil
}

// StartRPCServer registers RPC handlers and starts serving for this node.
func (n *NodeService) StartRPCServer() error {
	srv := rpc.NewServer()
	if err := srv.RegisterName("Node", n); err != nil {
		return fmt.Errorf("error registering RPCs: %w", err)
	}
	h := http.NewServeMux()
	h.Handle("/", srv)
	addr := fmt.Sprintf(":%d", n.Node.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listener error for node %d: %w", n.Node.Id, err)
	}
	n.logger.Log("Node %d serving RPC on port %d", n.Node.Id, n.Node.Port)
	go http.Serve(listener, h)
	return nil
}

// max helper
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
