package pipeline

import "time"

func (c Config) withDefaults() Config {
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 5
	}
	if c.BackoffInitial <= 0 {
		c.BackoffInitial = 10 * time.Millisecond
	}
	if c.BackoffMax <= 0 {
		c.BackoffMax = time.Second
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 4
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 5 * time.Millisecond
	}
	if c.Logger == nil {
		c.Logger = c.LoggerOrDefault()
	}
	return c
}
