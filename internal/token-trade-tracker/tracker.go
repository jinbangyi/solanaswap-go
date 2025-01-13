package tokentradetracker

import (
	"context"
	"fmt"
	"time"

	solanaswapgo "github.com/franco-bianco/solanaswap-go/solanaswap-go"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/gagliardetto/solana-go/rpc/ws"
	"github.com/jinbangyi/solanaswap-go/pkg/log"
	"github.com/panjf2000/ants"
	"go.uber.org/zap"

	tokentradeparser "github.com/jinbangyi/solanaswap-go/internal/token-trade-parser"
)

// Tracker interface defines methods for reading trade data from a Solana node
// type Tracker interface {
// 	// get history trades
// 	ReadTokenHistoricalTrades(tokenAddress solana.PublicKey) ([]Trade, error)
// 	ReadHistoricalTrades() ([]Trade, error)

// 	// get real-time trades
// 	ReadTokenRealTimeTrades(tokenAddress solana.PublicKey) (<-chan *Trade, error)
// 	ReadRealTimeTrades() (<-chan *Trade, error)

// 	// using the special strategy to prevent the unstable of the node
// 	GetClient() *ws.Client
// 	GetWSClient() *rpc.Client

// 	// inbuilt method
// 	// TODO define strategy interface
// 	ChangeClientStrategy() error
// }

type Tracker interface {
	ReadTrade() (<-chan *Trade, error)
	GetHttpClient() *rpc.Client
	GetWsClient() *ws.Client
	// tracker instance id
	String() string
	LogError(message string, err error)
}

// -------------------------------- BaseTracker --------------------------------

type BaseTracker struct {
	httpEndpoint []string
	wsEndpoint   []string

	wsClient  *ws.Client
	rpcClient *rpc.Client

	tokenAddress string
	trackerName  string
}

func (bt *BaseTracker) String() string {
	return fmt.Sprintf("%s token=%s", bt.trackerName, bt.tokenAddress)
}

func (bt *BaseTracker) LogError(message string, err error, fields ...zap.Field) {
	fields = append(fields, zap.String("Tracker", bt.String()))
	fields = append(fields, zap.Error(err))
	log.Error(message, fields...)
}

func (bt *BaseTracker) GetHttpClient() (*rpc.Client, error) {
	// TODO if client failed, retry different endpoint
	rpcClient := rpc.New(bt.httpEndpoint[0])

	bt.rpcClient = rpcClient
	return rpcClient, nil
}

func (bt *BaseTracker) GetWsClient() (*ws.Client, error) {
	// TODO if client failed, retry different endpoint
	wsClient, err := ws.Connect(context.Background(), bt.wsEndpoint[0])
	if err != nil {
		return nil, err
	}

	bt.wsClient = wsClient
	return wsClient, nil
}

func NewBaseTracker(httpEndpoint []string, wsEndpoint []string, tokenAddress string, trackerName string) (*BaseTracker, error) {
	if (len(httpEndpoint) == 0) || (len(wsEndpoint) == 0) {
		return nil, fmt.Errorf("httpEndpoint and wsEndpoint must not be empty")
	}

	tracker := &BaseTracker{
		httpEndpoint: httpEndpoint,
		wsEndpoint:   wsEndpoint,
		tokenAddress: tokenAddress,
		trackerName:  trackerName,
	}
	_, err := tracker.GetWsClient()
	if err != nil {
		log.Error("Error getting ws client", zap.Error(err))
		return nil, err
	}

	_, err = tracker.GetHttpClient()
	if err != nil {
		log.Error("Error getting http client", zap.Error(err))
		return nil, err
	}

	return tracker, nil
}

// -------------------------------- RealTimeTracker --------------------------------

// SolanaTracker is an implementation of the Tracker interface
type RealTimeTracker struct {
	BaseTracker
}

// if tokenAddress address is empty then subscribe to all token trades
func NewRealTimeTracker(httpEndpoint []string, wsEndpoint []string, tokenAddress string) (*RealTimeTracker, error) {
	baseTracker, err := NewBaseTracker(httpEndpoint, wsEndpoint, tokenAddress, "RealTimeTracker")
	if err != nil {
		return nil, err
	}

	tracker := &RealTimeTracker{
		BaseTracker: *baseTracker,
	}

	return tracker, nil
}

func (rtt *RealTimeTracker) ReadTrade() (<-chan *Trade, error) {
	var logSubscribe *ws.LogSubscription
	var err error

	if rtt.tokenAddress == "" {
		logSubscribe, err = rtt.wsClient.LogsSubscribe(
			ws.LogsSubscribeFilterAll,
			rpc.CommitmentConfirmed,
		)
	} else {
		logSubscribe, err = rtt.wsClient.LogsSubscribeMentions(
			solana.MustPublicKeyFromBase58(rtt.tokenAddress),
			rpc.CommitmentConfirmed,
		)
	}

	if err != nil {
		rtt.LogError("failed to subscribe logs", err)
		return nil, err
	}

	tradeChannel := make(chan *Trade, 100)

	go func() {
		ctx := context.Background()
		sleepTime := 1
		processTimeout := 5 * time.Second
		ticker := time.NewTicker(time.Duration(sleepTime)*time.Second + processTimeout)

		for {
			select {
			case <-ticker.C:
				log.Warn("Tracker progress timeout", zap.String("Tracker", rtt.String()))
			default:
				log.Info("start listening for trades: ", zap.Int("sleepTime", sleepTime), zap.String("tokenAddress", rtt.tokenAddress))
				// sleep some time before retrying
				if sleepTime > 0 {
					time.Sleep(time.Duration(sleepTime) * time.Second)
				}

				logResult, err := logSubscribe.Recv(ctx)
				if err != nil {
					// TODO if network issue, change the client else wait some time and retry
					log.Error("recv logs error", zap.Int("sleepTime", sleepTime), zap.Error(err))

					// sleep and retry
					if sleepTime == 0 {
						sleepTime = 1
					}
					sleepTime *= 2
					if sleepTime > 64 {
						sleepTime = 64
					}
					continue
				}

				log.Debug("signature: ", zap.String("signature", logResult.Value.Signature.String()))

				if logResult.Value.Err != nil {
					log.Debug("transaction error", zap.String("hash", logResult.Value.Signature.String()), zap.Any("error", logResult.Value.Err))
					// ignore the failed transaction
					continue
				}

				trade, err := rtt.GetTrade(logResult)
				if err != nil {
					rtt.LogError("get trade from logs error", err)
					// parse the log result failed
					continue
				}

				tradeChannel <- trade

				sleepTime = 0
				ticker.Reset(time.Duration(sleepTime)*time.Second + processTimeout)
			}
		}

		// ticker.Stop()
	}()

	return tradeChannel, nil
}

func (rtt *RealTimeTracker) GetTrade(logResult *ws.LogResult) (*Trade, error) {
	var maxTxVersion uint64 = 0

	// TODO speed up the get tx
	tx, err := rtt.rpcClient.GetTransaction(context.Background(), logResult.Value.Signature, &rpc.GetTransactionOpts{
		Commitment:                     rpc.CommitmentConfirmed,
		MaxSupportedTransactionVersion: &maxTxVersion,
	})
	if err != nil {
		rtt.LogError("error getting tx", err)
		return nil, err
	}

	parser, err := solanaswapgo.NewTransactionParser(tx)
	if err != nil {
		rtt.LogError("error creating parser", err)
		return nil, err
	}

	transactionData, err := parser.ParseTransaction()
	if err != nil {
		rtt.LogError("error parsing transaction", err)
		return nil, err
	}

	// marshalledData, err := json.MarshalIndent(transactionData, "", "  ")
	// if err != nil {
	// 	rtt.LogError("error marshalling transaction data", err)
	// 	return nil, err
	// }

	swapData, err := parser.ProcessSwapData(transactionData)
	if err != nil {
		rtt.LogError("error processing swap data", err)
		return nil, err
	}

	// marshalledSwapData, err := json.MarshalIndent(swapData, "", "  ")
	// if err != nil {
	// 	rtt.LogError("error marshalling swap data", err)
	// 	return nil, err
	// }
	return &Trade{
		SwapInfo: swapData,
		Tracker:  rtt.String(),
	}, nil
}

// -------------------------------- HistoryTracker --------------------------------

type HistoryTracker struct {
	BaseTracker
	startSlot uint64
	endSlot   uint64

	currentSlot uint64
}

func NewHistoryTracker(
	httpEndpoint []string,
	wsEndpoint []string,
	startSlot uint64,
	endSlot uint64,
) (*HistoryTracker, error) {
	baseTracker, err := NewBaseTracker(httpEndpoint, wsEndpoint, "", "HistoryTracker")
	if err != nil {
		return nil, err
	}

	tracker := &HistoryTracker{
		BaseTracker: *baseTracker,
		startSlot:   startSlot,
		endSlot:     endSlot,
		currentSlot: startSlot,
	}

	return tracker, nil
}

func (ht *HistoryTracker) WriteDataToChannel(currentSlot uint64, tradeChannel chan<- *Trade) error {
	ctx := context.Background()
	sleepTime := 1
	processTimeout := 5 * time.Second
	ticker := time.NewTicker(time.Duration(sleepTime)*time.Second + processTimeout)
	var rewards bool = false

	OuterLoop:
	for {
		select {
		case <-ticker.C:
			log.Warn("Tracker progress timeout", zap.String("Tracker", ht.String()))
		default:
			log.Info("start fetch trades of slot", zap.Uint64("slot", currentSlot))
			// sleep some time before retrying
			if sleepTime > 0 {
				time.Sleep(time.Duration(sleepTime) * time.Second)
			}

			// get all transactions
			blockResult, err := ht.rpcClient.GetBlockWithOpts(ctx, currentSlot, &rpc.GetBlockOpts{
				Commitment:                     rpc.CommitmentConfirmed,
				Rewards:                        &rewards,
				MaxSupportedTransactionVersion: &rpc.MaxSupportedTransactionVersion0,
			})
			if err != nil {
				// TODO if network issue, change the client else wait some time and retry
				ht.LogError("failed to get block info", err, zap.Int("sleepTime", sleepTime))

				// sleep and retry
				if sleepTime == 0 {
					sleepTime = 1
				}
				sleepTime *= 2
				if sleepTime > 64 {
					sleepTime = 64
				}
				continue
			}

			trades, err := ht.GetTrades(blockResult)
			if err != nil {
				ht.LogError("get trade from blocks error", err)
				// parse the log result failed
				continue
			}

			for _, trade := range trades {
				tradeChannel <- trade
			}

			log.Info("end fetch trades of slot", zap.Uint64("slot", currentSlot))
			break OuterLoop
		}
	}

	ticker.Stop()
	return nil
}

// TODO get token transactions
func (ht *HistoryTracker) ReadTrade() (<-chan *Trade, error) {
	tradeChannel := make(chan *Trade, 100000)

	go func() {
		// 10m, if one slot is 400ms, one slot~=2500tx, so 1500 slots ~= 3.8m
		var slotCount uint64 = 1500
		var poolNum int = 150
		var startSlot uint64 = ht.currentSlot
		var endSlot uint64 = ht.currentSlot

		pool, err := ants.NewPool(poolNum)
		if err != nil {
			log.Fatal("failed to create pool: %v", zap.Error(err))
			return
		}
		defer pool.Release()

		ticker := time.NewTicker(time.Duration(5) * time.Second)
		for endSlot < ht.endSlot {
			<-ticker.C
			log.Debug("waiting for workers end")
			if pool.Free() != 0 {
				startSlot = endSlot
				// startSlot + slotCount or ht.endSlot
				endSlot = startSlot + slotCount
				if endSlot > ht.endSlot {
					endSlot = ht.endSlot
				}
				ht.currentSlot = startSlot

				for i := startSlot; i < endSlot; i++ {
					slot := i
					err := pool.Submit(func() {
						err = ht.WriteDataToChannel(slot, tradeChannel)
						if err != nil {
							log.Error("failed to write data to channel", zap.Error(err))
							return
						}
					})
					if err != nil {
						ht.LogError("failed to submit pool", err, zap.Uint64("slot", slot))
						continue
					}
				}
			}
		}

		ticker.Stop()
	}()

	return tradeChannel, nil
}

func (rtt *HistoryTracker) GetTrades(logResult *rpc.GetBlockResult) ([]*Trade, error) {
	var trades []*Trade = make([]*Trade, 0)

	for i, tx := range logResult.Transactions {
		if tx.Meta.Err != nil {
			log.Debug("transaction error", zap.Uint64("slot", tx.Slot), zap.Int("index", i), zap.Any("error", tx.Meta.Err))
			// ignore the failed transaction
			continue
		}

		parsedTransaction, err := tx.GetTransaction()
		if err != nil {
			rtt.LogError("error getting transaction", err, zap.Uint64("slot", tx.Slot), zap.Int("index", i))
			continue
		}

		log.Debug("signature: ", zap.String("signature", parsedTransaction.Signatures[0].String()))

		parser, err := tokentradeparser.NewTransactionParserFromTransaction(parsedTransaction, tx.Meta)
		if err != nil {
			rtt.LogError("error creating parser", err)
			return nil, err
		}

		transactionData, err := parser.ParseTransaction()
		if err != nil {
			rtt.LogError("error parsing transaction", err)
			return nil, err
		}

		// marshalledData, err := json.MarshalIndent(transactionData, "", "  ")
		// if err != nil {
		// 	rtt.LogError("error marshalling transaction data", err)
		// 	return nil, err
		// }

		swapData, err := parser.ProcessSwapData(transactionData)
		if err != nil {
			rtt.LogError("error processing swap data", err)
			return nil, err
		}

		// marshalledSwapData, err := json.MarshalIndent(swapData, "", "  ")
		// if err != nil {
		// 	rtt.LogError("error marshalling swap data", err)
		// 	return nil, err
		// }
		trades = append(trades, &Trade{
			SwapInfo: &solanaswapgo.SwapInfo{
				Signers:    swapData.Signers,
				Signatures: swapData.Signatures,
				AMMs:       swapData.AMMs,
				Timestamp:  swapData.Timestamp,

				TokenInMint:     swapData.TokenInMint,
				TokenInAmount:   swapData.TokenInAmount,
				TokenInDecimals: swapData.TokenInDecimals,

				TokenOutMint:     swapData.TokenOutMint,
				TokenOutAmount:   swapData.TokenOutAmount,
				TokenOutDecimals: swapData.TokenOutDecimals,
			},
			Tracker: rtt.String(),
		})
	}

	return trades, nil
}
