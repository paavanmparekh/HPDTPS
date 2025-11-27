package consensus

import (
	configurations "2pc-paavanmparekh/Configurations"
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

const ()

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
}

func NewNodeService(node *configurations.Node) *NodeService {
	logger := nodelogger.GetLogger(node.Id)
	svc := &NodeService{
		Node:                   node,
		StartupPhase:           true,
		executionResults:       make(map[int]configurations.TxnReply),
		PendingClientResponses: make(map[int]chan configurations.TxnReply),
		WaitingRequests:        make(map[int]bool),
		locks:                  make(map[int]string),
		logger:                 logger,
		modifiedKeys:           make(map[int]bool),
	}
	svc.ElectionTimer = time.NewTimer(node.T)
	if svc.ElectionTimer.Stop() {
	}
	svc.TpTimer = time.NewTimer(node.Tp)
	go svc.startElectionLoop()
	return svc
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

// --- Utility helpers ---

func txnDedupKey(txn configurations.Transaction) string {
	// Use sender + logical timestamp + receiver/amount to distinguish retries
	return fmt.Sprintf("%d:%d:%d:%d:%t:%t", txn.Sender, txn.Receiver, txn.Amount, txn.Timestamp, txn.ReadOnly, txn.CreditOnly)
}

func lockOwnerKey(txn configurations.Transaction) string {
	return fmt.Sprintf("%d:%d:%d:%d:%t:%t", txn.Sender, txn.Timestamp, txn.Receiver, txn.Amount, txn.ReadOnly, txn.CreditOnly)
}

const (
	sqliteBusyRetries = 5
	sqliteBusyDelay   = 50 * time.Millisecond
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
	n.mu.RLock()
	defer n.mu.RUnlock()
	if status, ok := n.Node.TransactionStatus[seq]; ok {
		return status
	}
	return "unknown"
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
	port := configurations.GetNodePort(nodeID)
	if port == 0 {
		return 0, fmt.Errorf("port not found")
	}
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return 0, fmt.Errorf("unreachable")
	}
	defer client.Close()

	var balance int
	if err := client.Call("Node.GetBalance", key, &balance); err != nil {
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
			port := configurations.GetNodePort(target)
			addr := fmt.Sprintf("localhost:%d", port)
			client, err := rpc.DialHTTP("tcp", addr)
			if err != nil {
				n.logger.Log("[Node %v] Failed to connect to node %d: %v\n", B.NodeID, target, err)
				return
			}
			defer client.Close()

			var reply map[int]configurations.AcceptTxn
			err = client.Call("Node.NewView", newViewInput, &reply)
			if err != nil {
				n.logger.Log("[Node %v] NewView RPC to node %d failed: %v\n", B.NodeID, target, err)
			} else {
				mu.Lock()
				for key, value := range reply {
					CountAcceptedMsgs[key] += value.Acceptance
					SeqToTxnMap[key] = value
				}
				mu.Unlock()
			}
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
			port := configurations.GetNodePort(target)
			addr := fmt.Sprintf("localhost:%d", port)
			client, err := rpc.DialHTTP("tcp", addr)
			if err != nil {
				n.logger.Log("[Node %v] Failed to connect to node %d: %v\n", PB.NodeID, target, err)
				return
			}
			defer client.Close()

			var reply configurations.Promise
			err = client.Call("Node.Prepare", PB, &reply)
			if err != nil {
				n.logger.Log("[Node %v] Prepare RPC to node %d failed: %v\n", PB.NodeID, target, err)
			} else {
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
			}
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
		n.Node.Bnum.NodeID = 0
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
	if _, ok := n.Node.TransactionStatus[acceptObj.SeqNo]; !ok {
		n.Node.TransactionStatus[acceptObj.SeqNo] = "Committed"
	}

	n.BroadcastToCluster(func(targetNode int) {
		port := configurations.GetNodePort(targetNode)
		addr := fmt.Sprintf("localhost:%d", port)
		client, err := rpc.DialHTTP("tcp", addr)
		if err != nil {
			n.logger.Log("Failed to connect to node %d: %v\n", targetNode, err)
			return
		}
		defer client.Close()

		var reply configurations.TxnReply
		err = client.Call("Node.Commit", acceptObj, &reply)
		if err != nil {
			n.logger.Log("[Node %d] COMMIT RPC to Node %d failed: %v\n", n.Node.Id, targetNode, err)
		} else {
			n.logger.Log("[Node %d] COMMIT RPC to Node %d successful\n", n.Node.Id, targetNode)
		}
	})
	n.logger.Log("[PAXOS] Node %d: COMMIT broadcast done for seq=%d\n", n.Node.Id, acceptObj.SeqNo)
}

func (n *NodeService) BroadcastAcceptRPC(txn configurations.Transaction) *configurations.TxnReply {
	acceptance := 0
	var mu sync.Mutex
	var wg sync.WaitGroup

	txnReply := configurations.TxnReply{}
	n.mu.Lock()
	var acceptObj configurations.AcceptTxn
	acceptObj.Txn = txn
	acceptObj.B = n.Node.Bnum

	if n.Node.SequenceNumber < n.Node.LastExecuted {
		n.Node.SequenceNumber = n.Node.LastExecuted
	}
	n.Node.SequenceNumber++
	acceptObj.SeqNo = n.Node.SequenceNumber
	n.logger.Log("[PAXOS] Node %d: ACCEPT start seq=%d ballot=%v txn=%v->%v amt=%d\n", n.Node.Id, acceptObj.SeqNo, acceptObj.B, txn.Sender, txn.Receiver, txn.Amount)

	n.logger.Log("[Node %d] LEADER: Assigned SeqNo %d (LastExecuted=%d)\n", n.Node.Id, acceptObj.SeqNo, n.Node.LastExecuted)

	n.Node.AcceptLog = append(n.Node.AcceptLog, configurations.AcceptLog{AcceptNum: acceptObj.B, AcceptSeq: acceptObj.SeqNo, AcceptVal: acceptObj.Txn})
	if _, ok := n.Node.TransactionStatus[acceptObj.SeqNo]; !ok {
		n.Node.TransactionStatus[acceptObj.SeqNo] = "Accepted"
	}
	n.mu.Unlock()

	n.logger.Log("[Node %d] LEADER: Starting ACCEPT phase (Ballot: %v, SeqNo: %d)\n", n.Node.Id, n.Node.Bnum, acceptObj.SeqNo)
	for _, targetNode := range n.clusterNodes() {
		if targetNode == n.Node.Id {
			continue
		}

		wg.Add(1)
		go func(target int) {
			defer wg.Done()
			port := configurations.GetNodePort(target)
			addr := fmt.Sprintf("localhost:%d", port)
			client, err := rpc.DialHTTP("tcp", addr)
			if err != nil {
				n.logger.Log("Failed to connect to node %d: %v\n", target, err)
				return
			}
			defer client.Close()

			var reply configurations.AcceptTxn
			err = client.Call("Node.Accept", acceptObj, &reply)
			if err != nil {
				n.logger.Log("[Node %d] ACCEPT RPC to Node %d failed: %v\n", n.Node.Id, target, err)
			} else {
				n.logger.Log("[Node %d] ACCEPT RPC to Node %d returned: %d\n", n.Node.Id, target, reply.Acceptance)
				mu.Lock()
				acceptance += reply.Acceptance
				mu.Unlock()
			}
		}(targetNode)
	}

	wg.Wait()
	n.logger.Log("[Node %d] ACCEPT phase complete: %d acceptances (need %d)\n", n.Node.Id, acceptance+1, majority(len(n.clusterNodes())))
	if acceptance+1 >= majority(len(n.clusterNodes())) {
		n.logger.Log("[PAXOS] Node %d: ACCEPT majority for seq=%d\n", n.Node.Id, acceptObj.SeqNo)
		n.logger.Log("[Node %d] MAJORITY ACHIEVED: Proceeding to COMMIT phase\n", n.Node.Id)
		if _, ok := n.Node.TransactionStatus[acceptObj.SeqNo]; !ok {
			n.Node.TransactionStatus[acceptObj.SeqNo] = "Committed"
		}
		var txnRes configurations.TxnReply
		var wgExec sync.WaitGroup
		if n.Node.LastExecuted < acceptObj.SeqNo {
			wgExec.Add(1)
			go func() {
				n.ExecuteSerially(acceptObj, &txnRes)
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

			select {
			case actualResult := <-responseCh:
				txnRes = actualResult
				n.logger.Log("[Node %d] LEADER: Received actual execution result SeqNo %d: (%v %v)\n", n.Node.Id, acceptObj.SeqNo, txnRes.Res, txnRes.Msg)
			case <-time.After(n.Node.T):
				txnRes.Res = false
				txnRes.Msg = "Majority Not Accepted"
				n.logger.Log("[Node %d] LEADER: Timeout waiting for execution SeqNo %d\n", n.Node.Id, acceptObj.SeqNo)
				n.pendingMutex.Lock()
				delete(n.PendingClientResponses, acceptObj.SeqNo)
				n.pendingMutex.Unlock()
			}
		}

		n.logger.Log("[Node %d] LEADER: Executed locally SeqNo: %d, Result: (%v %v)\n", n.Node.Id, acceptObj.SeqNo, txnRes.Res, txnRes.Msg)
		n.BroadcastCommitRPC(acceptObj)
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

func (n *NodeService) ExecuteTxn(txn configurations.Transaction, status string) bool {
	if status == "no-op" || txn.Amount == 0 {
		return true
	}

	for attempt := 0; attempt < sqliteBusyRetries; attempt++ {
		tx, err := n.Node.Db.Begin()
		if err != nil {
			if isSQLiteBusy(err) {
				time.Sleep(sqliteBusyDelay)
				continue
			}
			n.logger.Log("[Node %d] DB begin failed: %v\n", n.Node.Id, err)
			return false
		}

		if txn.CreditOnly {
			n.logger.Log("[Node %d] CREDIT-ONLY: key=%d amount=%d\n", n.Node.Id, txn.Receiver, txn.Amount)
			if _, err = tx.Exec("UPDATE balances SET balance = balance + ? WHERE key = ?", txn.Amount, txn.Receiver); err != nil {
				tx.Rollback()
				if isSQLiteBusy(err) {
					time.Sleep(sqliteBusyDelay)
					continue
				}
				n.logger.Log("[Node %d] Credit-only failed: %v\n", n.Node.Id, err)
				return false
			}
		} else {
			var senderBalance int
			err = tx.QueryRow("SELECT balance FROM balances WHERE key = ?", txn.Sender).Scan(&senderBalance)
			if err != nil {
				tx.Rollback()
				if isSQLiteBusy(err) {
					time.Sleep(sqliteBusyDelay)
					continue
				}
				n.logger.Log("[Node %d] DB read failed: %v\n", n.Node.Id, err)
				return false
			}
			n.logger.Log("[Node %d] Account %d has balance %d, needs %d\n", n.Node.Id, txn.Sender, senderBalance, txn.Amount)
			if senderBalance < txn.Amount {
				tx.Rollback()
				n.logger.Log("[Node %d] Insufficient funds for %d\n", n.Node.Id, txn.Sender)
				return false
			}
			if _, err = tx.Exec("UPDATE balances SET balance = balance - ? WHERE key = ?", txn.Amount, txn.Sender); err != nil {
				tx.Rollback()
				if isSQLiteBusy(err) {
					time.Sleep(sqliteBusyDelay)
					continue
				}
				n.logger.Log("[Node %d] Debit failed: %v\n", n.Node.Id, err)
				return false
			}
			if _, err = tx.Exec("UPDATE balances SET balance = balance + ? WHERE key = ?", txn.Amount, txn.Receiver); err != nil {
				tx.Rollback()
				if isSQLiteBusy(err) {
					time.Sleep(sqliteBusyDelay)
					continue
				}
				n.logger.Log("[Node %d] Credit failed: %v\n", n.Node.Id, err)
				return false
			}
		}
		if err := tx.Commit(); err != nil {
			tx.Rollback()
			if isSQLiteBusy(err) {
				time.Sleep(sqliteBusyDelay)
				continue
			}
			n.logger.Log("[Node %d] Commit failed: %v\n", n.Node.Id, err)
			return false
		}

		n.modifiedKeysMutex.Lock()
		if !txn.CreditOnly {
			n.modifiedKeys[txn.Sender] = true
		}
		n.modifiedKeys[txn.Receiver] = true
		n.modifiedKeysMutex.Unlock()
		return true
	}

	n.logger.Log("[Node %d] ExecuteTxn: exceeded retry limit for %d->%d\n", n.Node.Id, txn.Sender, txn.Receiver)
	return false
}

func (n *NodeService) handleReadOnly(txn configurations.Transaction, reply *configurations.Reply) error {
	if !n.Node.IsLive {
		reply.Msg = "Node Not Live"
		return nil
	}
	n.logger.Log("[Node %d] READ-ONLY: key=%d ts=%d\n", n.Node.Id, txn.Sender, txn.Timestamp)
	var balance int
	err := n.Node.Db.QueryRow("SELECT balance FROM balances WHERE key = ?", txn.Sender).Scan(&balance)
	if err != nil {
		reply.Result = false
		reply.Msg = fmt.Sprintf("ReadFailed: %v", err)
	} else {
		reply.Result = true
		reply.Msg = fmt.Sprintf("Balance=%d", balance)
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

	n.mu.Lock()
	n.Node.TransactionStatus[acceptObj.SeqNo] = "Committed"
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
			if n.Node.TxnsProcessed != nil {
				if existing, ok := n.Node.TxnsProcessed[dedupKey]; ok && existing.Msg != "Majority Not Accepted" {
					cached = existing
					execNeeded = false
				}
			}
			if execNeeded {
				n.mu.Unlock()
				reply.Res = n.ExecuteTxn(cmd.Txn, cmd.Status)
				if reply.Res {
					reply.Msg = "Successful"
				} else {
					reply.Msg = "Failed"
				}
				n.mu.Lock()
				if n.Node.TxnsProcessed == nil {
					n.Node.TxnsProcessed = make(map[string]configurations.Reply)
				}
				n.Node.TxnsProcessed[dedupKey] = configurations.Reply{
					B:         cmd.B,
					Result:    reply.Res,
					Msg:       reply.Msg,
					Timestamp: cmd.Txn.Timestamp,
				}
			} else {
				reply.Res = cached.Result
				reply.Msg = cached.Msg
			}
			n.resultsMutex.Lock()
			n.executionResults[nextSeq] = *reply
			n.resultsMutex.Unlock()
			n.Node.TransactionStatus[cmd.SeqNo] = "Executed"
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
			n.releaseLocks(cmd.Txn)
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

// --- RPC handlers ---

func (n *NodeService) Commit(acceptObj configurations.AcceptTxn, reply *configurations.TxnReply) error {
	if !n.Node.IsLive {
		return nil
	}
	if n.Node.LastExecuted < acceptObj.SeqNo {
		n.ExecuteSerially(acceptObj, reply)
	}
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
		*reply = val
		n.mu.Unlock()
		return nil
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

	if !n.isNewView && n.Node.Bnum.NodeID == n.Node.Id {
		n.logger.Log("[Node %d] LEADER: Processing client request\n", n.Node.Id)
		txnRes := n.BroadcastAcceptRPC(txn)
		n.mu.Lock()
		reply.B.B = n.Node.Bnum.B
		reply.B.NodeID = n.Node.Id
		reply.Result = true
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

		if n.Node.SequenceNumber < accpetObj.SeqNo {
			n.Node.SequenceNumber = accpetObj.SeqNo
		}
		n.Node.AcceptLog = append(n.Node.AcceptLog, acceptEntry)
		if _, ok := n.Node.TransactionStatus[accpetObj.SeqNo]; !ok {
			n.Node.TransactionStatus[accpetObj.SeqNo] = "Accepted"
		}
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
			n.Node.AcceptLog = append(n.Node.AcceptLog, acceptEntry)

			(*acceptanceMap)[entry.AcceptSeq] = configurations.AcceptTxn{B: acceptEntry.AcceptNum, Txn: acceptEntry.AcceptVal, SeqNo: acceptEntry.AcceptSeq, Acceptance: 1, Status: acceptEntry.Status}
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
		n.logger.Log("[Node %d] STATUS: Node LIVE\n", n.Node.Id)
	} else {
		n.mu.Lock()
		n.Node.IsLive = false
		n.mu.Unlock()
		n.logger.Log("[Node %d] STATUS: Node FAILED\n", n.Node.Id)
	}
	*reply = true
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
		addr := fmt.Sprintf("localhost:%d", configurations.GetNodePort(peer))
		client, err := rpc.DialHTTP("tcp", addr)
		if err != nil {
			n.logger.Log("[Node %d] CATCH-UP: failed to dial n%d: %v\n", n.Node.Id, peer, err)
			continue
		}
		var snapshot configurations.StateSnapshot
		err = client.Call("Node.GetStateSnapshot", true, &snapshot)
		client.Close()
		if err != nil {
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
			if entry.AcceptSeq <= localLast {
				continue
			}
			acceptObj := configurations.AcceptTxn{
				B:          entry.AcceptNum,
				Txn:        entry.AcceptVal,
				SeqNo:      entry.AcceptSeq,
				Acceptance: 1,
				Status:     entry.Status,
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
	n.Node.TransactionStatus = make(map[int]string)
	n.Node.ClientLastReply = make(map[int]int)
	n.Node.TxnsProcessed = make(map[string]configurations.Reply)
	n.Node.Bnum.B = 1
	leaderID := configurations.GetClusterLeaderID(n.Node.ClusterId)
	n.Node.Bnum.NodeID = leaderID
	n.mu.Unlock()
	n.resultsMutex.Lock()
	n.executionResults = make(map[int]configurations.TxnReply)
	n.resultsMutex.Unlock()
	n.pendingMutex.Lock()
	n.PendingClientResponses = make(map[int]chan configurations.TxnReply)
	n.pendingMutex.Unlock()
	n.WaitingRequests = make(map[int]bool)
	n.locks = make(map[int]string)
	n.modifiedKeysMutex.Lock()
	modKeys := make([]int, 0, len(n.modifiedKeys))
	for k := range n.modifiedKeys {
		modKeys = append(modKeys, k)
	}
	n.modifiedKeys = make(map[int]bool)
	n.modifiedKeysMutex.Unlock()
	for _, key := range modKeys {
		n.Node.Db.Exec("UPDATE balances SET balance = 10 WHERE key = ?", key)
	}
	n.newViewMu.Lock()
	n.newViewMessages = nil
	n.newViewMu.Unlock()
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
