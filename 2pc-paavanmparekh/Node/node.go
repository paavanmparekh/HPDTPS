package main

import (
	configurations "2pc-paavanmparekh/Configurations"
	consensus "2pc-paavanmparekh/Node/Consensus"
	nodelogger "2pc-paavanmparekh/Node/logger"
	"bufio"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func NewNode(id, clusterId, port int) *configurations.Node {
	return &configurations.Node{
		Id:        id,
		ClusterId: clusterId,
		Port:      port,
	}
}

func ConnectDB(n *configurations.Node) error {
	dbPath := fmt.Sprintf("Database/node_n%d.db", n.Id)
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}
	n.Db = db
	return nil
}

func InitDB(n *configurations.Node) error {
	dbPath := fmt.Sprintf("Database/node_n%d.db", n.Id)
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}
	n.Db = db

	_, err = n.Db.Exec(`CREATE TABLE IF NOT EXISTS balances (key INTEGER PRIMARY KEY, balance INTEGER)`)
	if err != nil {
		return err
	}

	var keyStart, keyEnd int
	switch n.ClusterId {
	case 1:
		keyStart, keyEnd = 1, 3000
	case 2:
		keyStart, keyEnd = 3001, 6000
	case 3:
		keyStart, keyEnd = 6001, 9000
	}

	tx, err := n.Db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare("INSERT OR REPLACE INTO balances(key, balance) VALUES(?, ?)")
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()

	for key := keyStart; key <= keyEnd; key++ {
		_, err = stmt.Exec(key, 10)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

func StartNode(n *configurations.Node) *consensus.NodeService {
	svc := consensus.NewNodeService(n)
	if err := svc.StartRPCServer(); err != nil {
		log.Fatalf("Failed to start RPC server for n%d: %v", n.Id, err)
	}
	return svc
}

func main() {
	nodeID := flag.Int("id", 0, "Node ID to start")
	flag.Parse()

	id, clusterID, _ := configurations.GetNodeConfig(*nodeID)

	port := configurations.GetNodePort(id)
	node := NewNode(id, clusterID, port)

	// Initialize database if it doesn't exist
	err := InitDB(node)
	if err != nil {
		log.Fatalf("Failed to init DB for n%d: %v", id, err)
	}

	// Initialize consensus-related fields
	leaderID := configurations.GetClusterLeaderID(clusterID)
	node.Bnum = configurations.BallotNumber{B: 1, NodeID: leaderID}
	node.SequenceNumber = 0
	node.LastExecuted = 0
	node.IsLive = true
	node.T = 3 * time.Second
	node.Tp = node.T / 4
	node.TransactionStatus = make(map[int]string)
	node.ClientLastReply = make(map[int]int)
	node.TxnsProcessed = make(map[string]configurations.Reply)

	fmt.Printf("Node n%d starting on port %d in cluster C%d\n", id, port, clusterID)
	svc := StartNode(node)
	logger := nodelogger.GetLogger(id)
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Println("1: Print Log")
		fmt.Println("2: Print DB")
		fmt.Println("3: Print Transaction Status")
		fmt.Println("4: Print View")
		fmt.Println("5: Print Balance")
		fmt.Println("6: Clear Terminal")
		fmt.Println("7: Exit")
		choice, err := readIntInput(reader, "\nSelect an option: ")
		if err != nil {
			handleInputClosure(err, node, logger)
			return
		}

		switch choice {
		case 1:
			if err := logger.PrintLogContent(); err != nil {
				fmt.Printf("Failed to print log: %v\n", err)
			}
		case 2:
			svc.PrintDB()
		case 3:
			seq, err := readIntInput(reader, "Enter sequence number: ")
			if err != nil {
				handleInputClosure(err, node, logger)
				return
			}
			fmt.Printf("Seq %d status: %s\n", seq, svc.GetTransactionStatus(seq))
		case 4:
			svc.PrintView()
		case 5:
			key, err := readIntInput(reader, "Enter client/data item ID: ")
			if err != nil {
				handleInputClosure(err, node, logger)
				return
			}
			svc.PrintBalance(key)
		case 6:
			fmt.Print("\033[H\033[2J")
		case 7:
			CloseNode(node)
			logger.Close()
			return
		default:
			fmt.Println("Invalid choice")
		}
	}
}

func CloseNode(n *configurations.Node) {
	if n.Db != nil {
		n.Db.Close()
	}
}

func readIntInput(reader *bufio.Reader, prompt string) (int, error) {
	for {
		fmt.Print(prompt)
		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return 0, io.EOF
			}
			fmt.Printf("Input error: %v\n", err)
			continue
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		value, err := strconv.Atoi(line)
		if err != nil {
			fmt.Printf("Invalid number: %s\n", line)
			continue
		}
		return value, nil
	}
}

func handleInputClosure(err error, node *configurations.Node, logger *nodelogger.Logger) {
	if errors.Is(err, io.EOF) {
		fmt.Println("\nInput closed. Shutting down node console.")
	} else if err != nil {
		fmt.Printf("\nStopping console due to input error: %v\n", err)
	}
	CloseNode(node)
	logger.Close()
}
