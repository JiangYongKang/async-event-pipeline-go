// Package event defines the event envelope transported by the local pipeline.
package event

// Event is an immutable occurrence in the pipeline.
//
// ID is a client-supplied, globally unique identifier used for at-least-once
// delivery dedup: republishing the same ID never produces a second effect.
// Key is the business ordering key: events sharing a key are processed in the
// order they were produced; different keys may be processed concurrently.
// Seq is assigned by the local store at publish time and is unique per key.
type Event struct {
	ID      string
	Key     string
	Type    string
	Seq     uint64
	Payload []byte
}

// Delivery is one processing attempt of an event handed to a processor.
type Delivery struct {
	Event   Event
	Attempt int
}
