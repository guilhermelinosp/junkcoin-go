package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/guilhermelinosp/litecoin-go/blockchain"
	"github.com/guilhermelinosp/litecoin-go/consensus"
	"github.com/guilhermelinosp/litecoin-go/network"
	"github.com/libp2p/go-libp2p-core/peer"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize components
	bc := blockchain.NewBlockchain(4) // Difficulty of 4
	mp := blockchain.NewMempool()

	// Create libp2p node
	p2pNode, err := network.NewNode(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Start gRPC server
	go network.StartGRPCServer(bc, mp, p2pNode, "50051")

	// Initialize consensus
	consensus := consensus.NewConsensus(bc, mp, p2pNode)
	consensus.StartMining()
	defer consensus.StopMining()

	// Connect to bootstrap nodes if specified
	if len(os.Args) > 1 {
		for _, addr := range os.Args[1:] {
			peerInfo, err := peer.AddrInfoFromString(addr)
			if err != nil {
				continue
			}
			p2pNode.Connect(ctx, peerInfo)
		}
	}

	// Wait for shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("Shutting down...")
}
