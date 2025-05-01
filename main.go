package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/guilhermelinosp/litecoin-go/blockchain"
	"github.com/guilhermelinosp/litecoin-go/consensus"
	"github.com/guilhermelinosp/litecoin-go/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

func main() {
	// Define command-line flags
	grpcPort := flag.String("grpc-port", "50051", "Port for the gRPC server")
	p2pPort := flag.String("p2p-port", "4001", "Port for the P2P node")
	flag.Parse()

	// Remaining arguments are treated as bootstrap peer addresses
	bootstrapPeers := flag.Args()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize components
	bc := blockchain.NewBlockchain(4) // Difficulty of 4
	mp := blockchain.NewMempool()

	// Create libp2p node
	p2pNode, err := network.NewNode(ctx, *p2pPort) // Pass the port to the P2P node
	if err != nil {
		log.Fatalf("Failed to create P2P node: %v", err)
	}
	// Start gRPC server
	go network.StartGRPCServer(bc, mp, p2pNode, *grpcPort)

	// Initialize consensus
	consensus := consensus.NewConsensus(bc, mp, p2pNode)
	consensus.StartMining()
	defer consensus.StopMining()

	// Connect to bootstrap nodes if specified
	for _, addr := range bootstrapPeers {
		peerInfo, err := peer.AddrInfoFromString(addr)
		if err != nil {
			log.Printf("Failed to parse peer address %s: %v", addr, err)
			continue
		}
		if err := p2pNode.Connect(ctx, peerInfo); err != nil {
			log.Printf("Failed to connect to peer %s: %v", addr, err)
		}
	}

	// Wait for shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("Shutting down...")
}
