package tokentradetracker
// // package main

// /**
// Tracker: Read Trade from solana node
// 	- read history trade from solana node
// 	- read real-time trade from solana node
// 	- auto change the client using special strategy, so that to prevent the unstable of the node
// Dispatcher: read from tracker and dispatch to different handlers
// 	- get the progress of the history fetcher
// 	- start a new history fetcher
// 	- get the latency of the real-time fetcher
// Handler: handle trade
// 	- save to kafka
// */

// import (
// 	"context"
// 	"encoding/json"
// 	"fmt"

// 	// "github.com/davecgh/go-spew/spew"

// 	solanaswapgo "github.com/franco-bianco/solanaswap-go/solanaswap-go"
// 	"github.com/gagliardetto/solana-go"
// 	"github.com/gagliardetto/solana-go/rpc"
// 	"github.com/gagliardetto/solana-go/rpc/ws"
// 	"go.uber.org/zap"
// 	"github.com/panjf2000/ants/v2"

// 	"github.com/jinbangyi/solanaswap-go/pkg/log"
// )

// // some token < 10 or all token
// type TokenTradeTracker struct {
// 	// token address
// 	TokenAddress []string
// 	wsClient     *ws.Client
// 	rpcClient    *rpc.Client
// }

// func NewTokenTradeTracker(TokenAddress []string, wsEndpoint string, httpEndpoint string) *TokenTradeTracker {
// 	// TODO check token address is valid solana address
// 	// TODO connection pool listen for events
// 	wsClient, err := ws.Connect(context.Background(), wsEndpoint)
// 	if err != nil {
// 		panic(err)
// 	}

// 	rpcClient := rpc.New(httpEndpoint)

// 	return &TokenTradeTracker{
// 		wsClient:     wsClient,
// 		TokenAddress: TokenAddress,
// 		rpcClient:    rpcClient,
// 	}
// }

// // listen for token logs
// func (t *TokenTradeTracker) Listen(tokenAddress string) error {
// 	program := solana.MustPublicKeyFromBase58(tokenAddress)
// 	logSubscribe, err := t.wsClient.LogsSubscribeMentions(
// 		program,
// 		rpc.CommitmentConfirmed,
// 	)
// 	if err != nil {
// 		log.Error("failed to subscribe logs", zap.String("tokenAddress", tokenAddress), zap.Error(err))
// 		return err
// 	}

// 	ctx := context.Background()
// 	// ctx, cancel := context.WithCancel(context.Background())
// 	// defer cancel()

// 	go func() {
// 		for {
// 			log.Info("HTTP server started")
// 			logResult, err := logSubscribe.Recv(ctx)
// 			if err != nil {
// 				fmt.Println("recv logs error: ", err)
// 				// errCh <- fmt.Errorf("failed to get logs: %v", err)
// 				return
// 			}

// 			fmt.Println("signature: ", logResult.Value.Signature)

// 			// json logResult.Value.Err
// 			if logResult.Value.Err != nil {
// 				// spew.Dump(logResult.Value.Err)
// 				// fmt.Println("transaction error: ", logResult.Value.Err)
// 				// errCh <- fmt.Errorf("transaction failed: %v", logResult.Value.Err)
// 				continue
// 			}

// 			err = t.HandleLogResult(logResult)
// 			if err != nil {
// 				fmt.Println("handle logs error: ", err)
// 				// errCh <- fmt.Errorf("failed to handle log result: %v", err)
// 				return
// 			}
// 		}
// 	}()

// 	return nil

// 	// select {
// 	// case err := <-errCh:
// 	//
// 	//	log.Fatalf("error occurred: %v", err)
// 	//	return nil, err
// 	//
// 	// // case <-ctx.Done():
// 	// // 	log.Println("context cancelled")
// 	// // 	return nil, ctx.Err()
// 	// }
// }

// // handle log result
// func (t *TokenTradeTracker) HandleLogResult(logResult *ws.LogResult) error {
// 	// convert logresult to NewTransactionParser
// 	var maxTxVersion uint64 = 0

// 	tx, err := t.rpcClient.GetTransaction(context.Background(), logResult.Value.Signature, &rpc.GetTransactionOpts{
// 		Commitment:                     rpc.CommitmentConfirmed,
// 		MaxSupportedTransactionVersion: &maxTxVersion,
// 	})
// 	if err != nil {
// 		log.Fatalf("error getting tx: %s", err)
// 		return err
// 	}

// 	parser, err := solanaswapgo.NewTransactionParser(tx)
// 	if err != nil {
// 		log.Fatalf("error creating orca parser: %s", err)
// 		return err
// 	}

// 	transactionData, err := parser.ParseTransaction()
// 	if err != nil {
// 		log.Fatalf("error parsing transaction: %s", err)
// 	}

// 	marshalledData, err := json.MarshalIndent(transactionData, "", "  ")
// 	if err != nil {
// 		log.Fatalf("error marshalling transaction data: %s", err)
// 		return err
// 	}
// 	fmt.Println(string(marshalledData))

// 	swapData, err := parser.ProcessSwapData(transactionData)
// 	if err != nil {
// 		log.Fatalf("error processing swap data: %s", err)
// 		return err
// 	}

// 	marshalledSwapData, err := json.MarshalIndent(swapData, "", "  ")
// 	if err != nil {
// 		log.Fatalf("error marshalling swap data: %s", err)
// 		return err
// 	}
// 	fmt.Println(string(marshalledSwapData))
// 	return nil
// }

// // decode trade from logs
// func (t *TokenTradeTracker) DecodeTrade(logResult *ws.LogResult) (*Trade, error) {
// 	return nil, nil
// }

// func (t *TokenTradeTracker) Start() {
// 	pool, err := ants.NewPool(10)
// 	if err != nil {
// 		log.Fatal("failed to create pool: %v", zap.Error(err))
// 		return
// 	}
// 	defer pool.Release()

// 	for _, tokenAddress := range t.TokenAddress {
// 		pool.Submit(func() {
// 			err := t.Listen(tokenAddress)
// 			if err != nil {
// 				log.Fatal("failed to listen for token:", zap.String("token", tokenAddress), zap.Error(err))
// 			}
// 		})
// 	}

// 	// wait pool's workers end
// 	pool.Free()
// 	fmt.Println("waiting for workers")
// }

// func main() {
// 	TokenTradeTracker := NewTokenTradeTracker(
// 		[]string{"HeLp6NuQkmYB4pYWo2zYs22mESHXPQYzXbB8n4V98jwC"},
// 		// "wss://lb.drpc.org/ogws?network=solana&dkey=ApWQL9OFaUcAvu4fN3gvbNFnMK4A0ZQR75-2QkTKRtpJ",
// 		// "https://lb.drpc.org/ogrpc?network=solana&dkey=ApWQL9OFaUcAvu4fN3gvbNFnMK4A0ZQR75-2QkTKRtpJ",
// 		// "https://solana-mainnet.g.alchemy.com/v2/LLCv8Z62D7dXYdQNuWrTZIKcnitjb1Er",
// 		"wss://mainnet.helius-rpc.com/?api-key=6bfb79c3-fd6e-4a61-bae2-a097f611fa3a",
// 		"https://mainnet.helius-rpc.com/?api-key=6bfb79c3-fd6e-4a61-bae2-a097f611fa3a",
// 	)
// 	TokenTradeTracker.Start()
// }
