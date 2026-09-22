// Package business contains the example domain built on top of the pipeline:
// a minimal ordered account ledger used by the demo and the tests.
//
// Semantics exercised by the demo:
//   - account.open is required before money movement; re-opening is a fatal
//     business rejection (ALREADY_OPEN) and does not change the balance.
//   - account.withdraw beyond the balance is fatal (INSUFFICIENT_FUNDS).
//   - malformed payloads / unknown command types are fatal (BAD_COMMAND),
//     so a poison event reaches a terminal dead-letter instead of blocking
//     the key forever.
//   - hooks can force a number of transient failures per event ID, producing
//     distinguishable retry-then-success behavior with zero duplicate effects.
package business

import (
	"context"
	"encoding/json"

	"github.com/highcumontoa/async-event-pipeline-go/internal/event"
	"github.com/highcumontoa/async-event-pipeline-go/internal/pipeline"
)

// Command names carried in event.Type.
const (
	CmdOpen     = "account.open"
	CmdDeposit  = "account.deposit"
	CmdWithdraw = "account.withdraw"
)

// Account-specific, distinguishable failure reasons (they propagate verbatim
// into DeadRecord.Reason).
const (
	ReasonAlreadyOpen  = "ALREADY_OPEN"
	ReasonNotOpen      = "ACCOUNT_NOT_OPEN"
	ReasonInsufficient = "INSUFFICIENT_FUNDS"
	ReasonBadCommand   = "BAD_COMMAND"
	ReasonTransient    = "TRANSIENT_FAILURE"
)

// Command is the JSON payload of account events.
type Command struct {
	Account string `json:"account"`
	Amount  int64  `json:"amount"`
}

// BalanceKey is the business-state KV key of an account balance.
func BalanceKey(account string) string { return "acct:" + account + ":bal" }

// OpenedKey records that an account has been opened.
func OpenedKey(account string) string { return "acct:" + account + ":open" }

// FlakyHooks injects a deterministic number of transient failures for chosen
// event IDs so retry behavior is testable without any network dependency.
type FlakyHooks interface {
	// FailNTimes returns how many early attempts for id must fail.
	FailNTimes(id string) int
	// RecordAttempt observes one actual attempt (for assertions/logs).
	RecordAttempt(id string, attempt int)
}

// AccountProcessor builds a pipeline.Processor applying account commands.
func AccountProcessor(hooks FlakyHooks) pipeline.Processor {
	return func(ctx context.Context, tx pipeline.Tx, d event.Delivery) error {
		if hooks != nil {
			hooks.RecordAttempt(d.Event.ID, d.Attempt)
			if d.Attempt <= hooks.FailNTimes(d.Event.ID) {
				return pipeline.Retryf("%s: forced transient failure on attempt %d", ReasonTransient, d.Attempt)
			}
		}

		var cmd Command
		if len(d.Event.Payload) > 0 {
			if err := json.Unmarshal(d.Event.Payload, &cmd); err != nil {
				return pipeline.Fatalf("%s: payload not json: %v", ReasonBadCommand, err)
			}
		}
		if cmd.Account == "" {
			cmd.Account = d.Event.Key
		}
		if cmd.Amount < 0 {
			return pipeline.Fatalf("%s: negative amount %d", ReasonBadCommand, cmd.Amount)
		}

		balKey, openKey := BalanceKey(cmd.Account), OpenedKey(cmd.Account)
		balance := readBalance(tx, balKey)
		_, opened := tx.Get(openKey)

		switch d.Event.Type {
		case CmdOpen:
			if opened {
				return pipeline.Fatalf("%s: account %q already open", ReasonAlreadyOpen, cmd.Account)
			}
			tx.Put(openKey, []byte("1"))
			tx.Put(balKey, encodeInt(balance)) // explicit opening balance (0 unless given)
			return nil
		case CmdDeposit:
			if !opened {
				return pipeline.Fatalf("%s: account %q not open", ReasonNotOpen, cmd.Account)
			}
			tx.Put(balKey, encodeInt(balance+cmd.Amount))
			return nil
		case CmdWithdraw:
			if !opened {
				return pipeline.Fatalf("%s: account %q not open", ReasonNotOpen, cmd.Account)
			}
			if balance < cmd.Amount {
				return pipeline.Fatalf("%s: account %q balance %d < withdraw %d",
					ReasonInsufficient, cmd.Account, balance, cmd.Amount)
			}
			tx.Put(balKey, encodeInt(balance-cmd.Amount))
			return nil
		default:
			return pipeline.Fatalf("%s: unknown event type %q", ReasonBadCommand, d.Event.Type)
		}
	}
}
