package logger

import (
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
)

type Logger struct {
	file   *os.File
	logger *log.Logger
	mu     sync.Mutex
	muted  uint32
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
	if atomic.LoadUint32(&l.muted) == 1 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logger.Printf(format, args...)
}

func (l *Logger) SetMuted(enabled bool) {
	if enabled {
		atomic.StoreUint32(&l.muted, 1)
		return
	}
	atomic.StoreUint32(&l.muted, 0)
}

func (l *Logger) Clear() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return nil
	}
	if err := l.file.Truncate(0); err != nil {
		return err
	}
	if _, err := l.file.Seek(0, 0); err != nil {
		return err
	}
	l.logger = log.New(l.file, "", log.LstdFlags)
	return nil
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
