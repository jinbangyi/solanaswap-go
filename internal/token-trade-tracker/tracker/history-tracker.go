package tokentradetracker

import (
	"context"
	"time"

	"github.com/gagliardetto/solana-go/rpc"
	"github.com/jinbangyi/solanaswap-go/internal/token-trade-tracker/btype"
	tokentradeparser "github.com/jinbangyi/solanaswap-go/pkg/token-trade-parser"
	"github.com/panjf2000/ants"
	"go.uber.org/zap"
)

// -------------------------------- HistoryTracker --------------------------------

// TODO ensure all transaction parsed and collect failed slot and transactions
type HistoryTracker struct {
	*BaseTracker
	startSlot uint64
	endSlot   uint64

	// show the process
	currentSlot uint64
	failedSlots []uint64
	failedTx    []string
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
		BaseTracker: baseTracker,
		startSlot:   startSlot,
		endSlot:     endSlot,
		currentSlot: startSlot,
		failedSlots: make([]uint64, 0),
		failedTx:    make([]string, 0),
	}

	return tracker, nil
}

func (ht *HistoryTracker) run(currentSlot uint64, tradeChannel chan<- *Trade) error {
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

	log.Warn("failed transactions", zap.Any("failedTx", ht.failedTx))
	ticker.Stop()
	return nil
}

// TODO get token transactions
func (ht *HistoryTracker) ReadTrade(ctx context.Context) (<-chan *btype.Trade, error) {
	tradeChannel := make(chan *btype.Trade, ht.channelBufferSize)

	go func() {
		// 10m, if one slot is 400ms, one slot~=2500tx, so 1500 slots ~= 3.8m
		var slotCount uint64 = 1500
		var poolNum int = 150
		var startSlot uint64 = ht.currentSlot
		var endSlot uint64 = ht.currentSlot
		processTimeout := 5 * time.Second

		pool, err := ants.NewPool(poolNum)
		if err != nil {
			ht.LogError("failed to create pool", err)
			return
		}
		defer pool.Release()

		ticker := time.NewTicker(processTimeout)

		for endSlot < ht.endSlot {
			<-ticker.C

			if pool.Free() != 0 {
				startSlot = endSlot
				// startSlot + slotCount or ht.endSlot
				endSlot = startSlot + slotCount
				if endSlot > ht.endSlot {
					endSlot = ht.endSlot
				}
				ht.currentSlot = startSlot

				ht.LogDebug("add new task:", zap.Int("poolFree", pool.Free()), zap.Uint64("startSlot", startSlot), zap.Uint64("endSlot", endSlot))

				for i := startSlot; i < endSlot; i++ {
					slot := i
					err := pool.Submit(func() {
						err = ht.run(slot, tradeChannel)
						if err != nil {
							ht.LogError("failed to write data to channel", zap.Error(err))
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

		ht.LogDebug("waiting for workers end")

		ticker.Stop()
	}()

	return tradeChannel, nil
}

func (ht *HistoryTracker) ParseTrade(getBlockResult *rpc.GetBlockResult) (*btype.Trade, error) {
	var trades []*btype.Trade = make([]*btype.Trade, 0)

	for i, tx := range getBlockResult.Transactions {
		if tx.Meta.Err != nil {
			log.Debug("transaction error", zap.Uint64("slot", tx.Slot), zap.Int("index", i), zap.Any("error", tx.Meta.Err))
			// ignore the failed transaction
			continue
		}

		parsedTransaction, err := tx.GetTransaction()
		if err != nil {
			ht.LogError("error getting transaction", err, zap.Uint64("slot", tx.Slot), zap.Int("index", i))
			ht.failedTx = append(ht.failedTx, getBlockResult.Signatures[i].String())
			continue
		}

		log.Debug("signature: ", zap.String("signature", parsedTransaction.Signatures[0].String()))

		parser, err := tokentradeparser.NewTransactionParserFromTransaction(parsedTransaction, tx.Meta)
		if err != nil {
			ht.LogError("error creating parser", err)
			ht.failedTx = append(ht.failedTx, getBlockResult.Signatures[i].String())
			continue
		}

		transactionData, err := parser.ParseTransaction()
		if err != nil {
			ht.LogError("error parsing transaction", err)
			ht.failedTx = append(ht.failedTx, getBlockResult.Signatures[i].String())
			continue
		}

		// marshalledData, err := json.MarshalIndent(transactionData, "", "  ")
		// if err != nil {
		// 	ht.LogError("error marshalling transaction data", err)
		// 	return nil, err
		// }

		swapData, err := parser.ProcessSwapData(transactionData)
		if err != nil {
			ht.LogError("error processing swap data", err)
			ht.failedTx = append(ht.failedTx, getBlockResult.Signatures[i].String())
			continue
		}

		// marshalledSwapData, err := json.MarshalIndent(swapData, "", "  ")
		// if err != nil {
		// 	ht.LogError("error marshalling swap data", err)
		// 	return nil, err
		// }
		trades = append(trades, NewTrade(
			ht.String(),
			&solanaswapgo.SwapInfo{
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
			tx.Slot,
		))
	}

	return trades, nil
}
