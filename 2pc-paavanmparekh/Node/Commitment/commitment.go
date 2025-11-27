package commitment

import (
	configurations "2pc-paavanmparekh/Configurations"
	"database/sql"
	"fmt"
	"sync"
	"time"
)

type PrepareRequest struct {
	Txn                 configurations.Transaction
	TxnID               string
	CoordinatorID       int
	CoordinatorSeq      int
	CoordinatorCluster  int
	ParticipantCluster  int
	ParticipantLeaderID int
	Timestamp           int
}

type PrepareResponse struct {
	TxnID            string
	Ready            bool
	ParticipantSeq   int
	Reason           string
	ParticipantPhase configurations.TwoPCPhase
	LeaderID         int
}

type DecisionNotification struct {
	TxnID          string
	Decision       configurations.TwoPCPhase
	CoordinatorSeq int
	ParticipantSeq int
}

type TransactionState struct {
	Txn                  configurations.Transaction
	TxnID                string
	CoordinatorSeq       int
	ParticipantSeq       int
	CoordinatorID        int
	ParticipantLeaderID  int
	Role                 configurations.TwoPCRole
	Phase                configurations.TwoPCPhase
	Decision             configurations.TwoPCPhase
	CoordinatorCluster   int
	ParticipantCluster   int
	CreatedAt            time.Time
	Deadline             time.Time
	Timer                *time.Timer
	LastError            string
	AcknowledgedDecision bool
}

type WALRecord struct {
	TxnID   string
	Key     int
	Value   int
	Phase   configurations.TwoPCPhase
	Role    configurations.TwoPCRole
	Created time.Time
}

type WALManager struct {
	db *sql.DB
	mu sync.Mutex
}

func NewWALManager(db *sql.DB) *WALManager {
	return &WALManager{db: db}
}

func (w *WALManager) EnsureTables() error {
	if w == nil || w.db == nil {
		return fmt.Errorf("wal manager not initialized")
	}
	_, err := w.db.Exec(`CREATE TABLE IF NOT EXISTS wal_records (
		txn_id TEXT NOT NULL,
		key INTEGER NOT NULL,
		before_value INTEGER NOT NULL,
		phase TEXT NOT NULL,
		role TEXT NOT NULL,
		created_at INTEGER NOT NULL,
		PRIMARY KEY(txn_id, key, phase, role)
	)`)
	return err
}

func (w *WALManager) Record(txnID string, key int, before int, phase configurations.TwoPCPhase, role configurations.TwoPCRole) error {
	if w == nil || w.db == nil {
		return fmt.Errorf("wal manager not initialized")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err := w.db.Exec(`INSERT OR REPLACE INTO wal_records(txn_id, key, before_value, phase, role, created_at) VALUES(?, ?, ?, ?, ?, ?)`,
		txnID, key, before, string(phase), string(role), time.Now().UnixNano())
	return err
}

func (w *WALManager) RecordTx(tx *sql.Tx, txnID string, key int, before int, phase configurations.TwoPCPhase, role configurations.TwoPCRole) error {
	if w == nil {
		return fmt.Errorf("wal manager not initialized")
	}
	if tx == nil {
		return w.Record(txnID, key, before, phase, role)
	}
	_, err := tx.Exec(`INSERT OR REPLACE INTO wal_records(txn_id, key, before_value, phase, role, created_at) VALUES(?, ?, ?, ?, ?, ?)`,
		txnID, key, before, string(phase), string(role), time.Now().UnixNano())
	return err
}

func (w *WALManager) BeforeImages(txnID string) (map[int]int, error) {
	if w == nil || w.db == nil {
		return nil, fmt.Errorf("wal manager not initialized")
	}
	rows, err := w.db.Query(`SELECT key, before_value FROM wal_records WHERE txn_id = ?`, txnID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	images := make(map[int]int)
	for rows.Next() {
		var key, before int
		if err := rows.Scan(&key, &before); err != nil {
			return nil, err
		}
		images[key] = before
	}
	return images, rows.Err()
}

func (w *WALManager) Clear(txnID string) error {
	if w == nil || w.db == nil {
		return fmt.Errorf("wal manager not initialized")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err := w.db.Exec(`DELETE FROM wal_records WHERE txn_id = ?`, txnID)
	return err
}

type DecisionRequest struct {
	TxnID              string
	Decision           configurations.TwoPCPhase
	CoordinatorSeq     int
	ParticipantSeq     int
	CoordinatorCluster int
	ParticipantCluster int
	Txn                configurations.Transaction
	Round              int
}

type DecisionResponse struct {
	TxnID  string
	Ack    bool
	Reason string
}
