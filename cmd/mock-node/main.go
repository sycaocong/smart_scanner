package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/smart-scanner/multi-chain-scanner/internal/node"
)

func main() {
	chainID := flag.Uint64("chain-id", 1, "Chain ID (1=Ethereum, 5=Goerli, 56=BSC)")
	port := flag.Int("port", 8545, "RPC port")
	flag.Parse()

	mockNode := node.NewMockEVMNode(*chainID, *port)
	
	if err := mockNode.Start(); err != nil {
		fmt.Printf("Failed to start mock node: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\nMock EVM Node is running:\n")
	fmt.Printf("  Chain ID: %d\n", *chainID)
	fmt.Printf("  RPC URL: http://localhost:%d\n", *port)
	fmt.Printf("  Current Block: %d\n", mockNode.GetCurrentHeight())
	fmt.Printf("\nAvailable RPC Methods:\n")
	fmt.Printf("  - eth_blockNumber\n")
	fmt.Printf("  - eth_getBlockByNumber\n")
	fmt.Printf("  - eth_getBlockByHash\n")
	fmt.Printf("  - eth_getTransactionByHash\n")
	fmt.Printf("  - eth_getTransactionReceipt\n")
	fmt.Printf("  - eth_getLogs\n")
	fmt.Printf("  - eth_chainId\n")
	fmt.Printf("  - net_version\n")
	fmt.Printf("\nPress Ctrl+C to stop...\n")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	<-sigChan
	
	fmt.Println("\nStopping mock node...")
	if err := mockNode.Stop(); err != nil {
		fmt.Printf("Error stopping node: %v\n", err)
	}
	
	fmt.Println("Mock node stopped successfully")
}