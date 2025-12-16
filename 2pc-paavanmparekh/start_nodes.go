package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

type clusterConfig struct {
	ClusterCount    int `json:"clusterCount"`
	NodesPerCluster int `json:"nodesPerCluster"`
}

func main() {
	defaultConfig := filepath.Join("Configurations", "cluster_config.json")
	defaultNode := filepath.Join("Node", "Node.exe")

	configPath := flag.String("config", defaultConfig, "Path to cluster_config.json")
	nodePath := flag.String("node", defaultNode, "Path to Node executable")
	terminalPath := flag.String("terminal", "wt.exe", "Windows Terminal executable (wt.exe)")
	flag.Parse()

	if runtime.GOOS != "windows" {
		exitWithError(errors.New("start_nodes.go requires Windows (wt.exe tabs)"))
	}

	cfg, err := loadClusterConfig(*configPath)
	if err != nil {
		exitWithError(err)
	}

	if cfg.ClusterCount <= 0 || cfg.NodesPerCluster <= 0 {
		exitWithError(fmt.Errorf("invalid cluster configuration: clusterCount=%d nodesPerCluster=%d",
			cfg.ClusterCount, cfg.NodesPerCluster))
	}

	nodeExeAbs, err := filepath.Abs(*nodePath)
	if err != nil {
		exitWithError(fmt.Errorf("resolve node executable: %w", err))
	}
	if _, err := os.Stat(nodeExeAbs); err != nil {
		exitWithError(fmt.Errorf("node executable not found at %s: %w", nodeExeAbs, err))
	}

	wtPath, err := resolveExecutable(*terminalPath)
	if err != nil {
		exitWithError(fmt.Errorf("resolve Windows Terminal executable: %w", err))
	}

	nodeDir := filepath.Dir(nodeExeAbs)
	totalNodes := cfg.ClusterCount * cfg.NodesPerCluster

	args := buildWindowsTerminalArgs(nodeDir, nodeExeAbs, totalNodes)

	cmd := exec.Command(wtPath, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		exitWithError(fmt.Errorf("launch Windows Terminal: %w", err))
	}
	fmt.Printf("Launched %d node terminal(s) in Windows Terminal.\n", totalNodes)
}

func loadClusterConfig(path string) (clusterConfig, error) {
	var cfg clusterConfig
	bytes, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("read cluster config %s: %w", path, err)
	}
	if err := json.Unmarshal(bytes, &cfg); err != nil {
		return cfg, fmt.Errorf("parse cluster config %s: %w", path, err)
	}
	return cfg, nil
}

func resolveExecutable(path string) (string, error) {
	if filepath.IsAbs(path) {
		if _, err := os.Stat(path); err != nil {
			return "", err
		}
		return path, nil
	}
	if full, err := exec.LookPath(path); err == nil {
		return full, nil
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(abs); err != nil {
		return "", err
	}
	return abs, nil
}

func buildWindowsTerminalArgs(nodeDir, nodeExeAbs string, totalNodes int) []string {
	args := make([]string, 0, totalNodes*8)
	for nodeID := 1; nodeID <= totalNodes; nodeID++ {
		if nodeID > 1 {
			args = append(args, ";")
		}
		command := fmt.Sprintf("cd /d \"%s\" && \"%s\" -id %d",
			escapeCmdArg(nodeDir), escapeCmdArg(nodeExeAbs), nodeID)
		args = append(args,
			"new-tab",
			"--title", fmt.Sprintf("n%d", nodeID),
			"--",
			"cmd.exe",
			"/k",
			command,
		)
	}
	return args
}

func escapeCmdArg(val string) string {
	return strings.ReplaceAll(val, "\"", "\\\"")
}

func exitWithError(err error) {
	fmt.Fprintf(os.Stderr, "start_nodes: %v\n", err)
	os.Exit(1)
}
