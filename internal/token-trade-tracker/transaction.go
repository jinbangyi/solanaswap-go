package tradetracker

/**
This package will return all trade of a token
*/

import (
	"context"
	"fmt"
    "log"

	"github.com/davecgh/go-spew/spew"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/gagliardetto/solana-go/rpc/ws"
)

func main() {
	ctx := context.Background()

	// https://solana.drpc.org

	client, err := ws.Connect(context.Background(), rpc.MainNetBeta_WS)
	if err != nil {
		panic(err)
	}

	// Get the latest slot
	latestSlot, err := client.sl(ctx, rpc.CommitmentFinalized)
	if err != nil {
		log.Fatalf("failed to get latest slot: %v", err)
	}

	program := solana.MustPublicKeyFromBase58("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA") // token

	sub, err := client.ProgramSubscribeWithOpts(
		program,
		rpc.CommitmentRecent,
		solana.EncodingBase64Zstd,
		nil,
	)
	if err != nil {
		panic(err)
	}
	defer sub.Unsubscribe()

	for {
		got, err := sub.Recv(ctx)
		if err != nil {
			panic(err)
		}
		spew.Dump(got)

		decodedBinary := got.Value.Account.Data.GetBinary()
		if decodedBinary != nil {
			// spew.Dump(decodedBinary)
		}

		// or if you requested solana.EncodingJSONParsed and it is supported:
		rawJSON := got.Value.Account.Data.GetRawJSON()
		if rawJSON != nil {
			// spew.Dump(rawJSON)
		}
	}
}
