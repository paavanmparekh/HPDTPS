package logger

import (
	"fmt"
	"log"
	"os"
	"sync"
)

type Logger struct {
	file   *os.File
	logger *log.Logger
	mu     sync.Mutex
}

var loggers = make(map[int]*Logger)
var loggerMu sync.Mutex

func GetLogger(nodeID int) *Logger {
	loggerMu.Lock()
	defer loggerMu.Unlock()

	if logger, exists := loggers[nodeID]; exists {
		return logger
	}

	os.MkdirAll("Logs", 0755)
	filename := fmt.Sprintf("Logs/PrintLog_%d.txt", nodeID)
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalf("Failed to create log file for node %d: %v", nodeID, err)
	}

	logger := &Logger{
		file:   file,
		logger: log.New(file, "", log.LstdFlags),
	}
	loggers[nodeID] = logger
	return logger
}

func (l *Logger) Log(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logger.Printf(format, args...)
}

func (l *Logger) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file != nil {
		l.file.Close()
	}
}

func (l *Logger) PrintLogContent() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	filename := l.file.Name()
	l.file.Close()

	content, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("failed to read log file: %v", err)
	}
	fmt.Print(string(content))

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return fmt.Errorf("failed to reopen log file: %v", err)
	}
	l.file = file
	l.logger = log.New(file, "", log.LstdFlags)
	return nil
}
