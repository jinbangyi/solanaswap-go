package tracker

import (
	"context"
	"fmt"
	"time"

	"github.com/gagliardetto/solana-go/rpc"
	"github.com/jinbangyi/solanaswap-go/internal/token-trade-tracker/btype"
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

	pool *ants.Pool
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

	var poolNum int = 150
	pool, err := ants.NewPool(poolNum)
	if err != nil {
		return nil, fmt.Errorf("failed to create pool: %w", err)
	}

	tracker := &HistoryTracker{
		BaseTracker: baseTracker,
		startSlot:   startSlot,
		endSlot:     endSlot,
		currentSlot: startSlot,
		failedSlots: make([]uint64, 0),
		failedTx:    make([]string, 0),
		pool:        pool,
	}

	return tracker, nil
}

func (ht *HistoryTracker) run(currentSlot uint64) error {
	ctx := context.Background()
	var rewards bool = false

	ht.LogInfo("start fetch trades of slot", zap.Uint64("slot", currentSlot))

	// get all transactions
	blockResult, err := ht.rpcClient.GetBlockWithOpts(ctx, currentSlot, &rpc.GetBlockOpts{
		Commitment:                     rpc.CommitmentConfirmed,
		Rewards:                        &rewards,
		MaxSupportedTransactionVersion: &rpc.MaxSupportedTransactionVersion0,
	})
	if err != nil {
		// TODO if network issue, change the client else wait some time and retry
		return fmt.Errorf("failed to get block info: %w", err)
	}

	trades, err := ht.parseTrades(context.Background(), blockResult)
	if err != nil {
		ht.LogError("get trade from blocks error", err)
		// parse the log result failed
		return nil
	}

	for _, trade := range trades {
		ht.tradeChannel <- trade
	}

	ht.LogInfo("end fetch trades of slot", zap.Uint64("slot", currentSlot))

	return nil
}

// TODO get token transactions
func (ht *HistoryTracker) ReadTrade(ctx context.Context) (<-chan *btype.Trade, error) {
	go func() {
		// 10m, if one slot is 400ms, one slot~=2500tx, so 1500 slots ~= 3.8m
		var slotCount uint64 = 1500
		var endSlot uint64 = ht.startSlot
		checkPoolInterval := 5 * time.Second
		ticker := time.NewTicker(checkPoolInterval)

		for endSlot < ht.endSlot {
			// TODO using wggroup
			<-ticker.C

			if ht.pool.Free() != 0 {
				startSlot := endSlot
				// startSlot + slotCount or ht.endSlot
				endSlot = startSlot + slotCount
				if endSlot > ht.endSlot {
					endSlot = ht.endSlot
				}
				ht.currentSlot = startSlot

				ht.LogDebug("add new task:", zap.Int("poolFree", ht.pool.Free()), zap.Uint64("startSlot", startSlot), zap.Uint64("endSlot", endSlot))

				for i := startSlot; i < endSlot; i++ {
					slot := i
					err := ht.pool.Submit(func() {
						err := ht.FuncTimeoutAndContextDoneWrapper(ctx, time.Duration(10*time.Second), func() error {
							return ht.run(slot)
						})
						// err = ht.run(slot, tradeChannel)
						if err != nil {
							ht.LogError("failed to write data to channel", err)
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

	return ht.tradeChannel, nil
}

func (ht *HistoryTracker) parseTrades(ctx context.Context, getBlockResult *rpc.GetBlockResult) ([]*btype.Trade, error) {
	var trades []*btype.Trade = make([]*btype.Trade, 0)

	for i, tx := range getBlockResult.Transactions {
		trade, err := ht.ParseTrade(ctx, &tx)
		if err != nil {
			ht.LogError("failed parse trade", err, zap.Uint64("slot", tx.Slot), zap.Int("index", i))
			ht.failedTx = append(ht.failedTx, getBlockResult.Signatures[i].String())
			continue
		}

		// marshalledSwapData, err := json.MarshalIndent(swapData, "", "  ")
		// if err != nil {
		// 	ht.LogError("error marshalling swap data", err)
		// 	return nil, err
		// }
		trades = append(trades, trade)
	}

	return trades, nil
}

func (ht *HistoryTracker) ParseTrade(ctx context.Context, tx *rpc.TransactionWithMeta) (*btype.Trade, error) {
	if tx.Meta.Err != nil {
		ht.LogDebug("transaction error", zap.Uint64("slot", tx.Slot), zap.Any("error", tx.Meta.Err))
		// ignore the failed transaction
		return nil, nil
	}

	parsedTransaction, err := tx.GetTransaction()
	if err != nil {
		return nil, fmt.Errorf("error getting tx: %w", err)
	}

	ht.LogDebug("signature: ", zap.String("signature", parsedTransaction.Signatures[0].String()))

	return ht.parseTrade(
		ctx,
		rpc.TransactionParsed{
			Transaction: parsedTransaction,
			Meta:        tx.Meta,
		},
		tx.Slot,
	)
}
