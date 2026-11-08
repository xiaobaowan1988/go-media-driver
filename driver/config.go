package driver

import "time"

// Timing holds the protocol timers shared by publications and subscriptions.
type Timing struct {
	HeartbeatInterval  time.Duration // sender: idle heartbeat
	SMInterval         time.Duration // receiver: periodic status message
	NakInterval        time.Duration // receiver: min gap between NAKs for the same hole
	RetransmitLinger   time.Duration // sender: ignore duplicate NAKs for the same start
	HousekeepingPeriod time.Duration // conductor
}

// DefaultTiming suits a LAN / VPC.
func DefaultTiming() Timing {
	return Timing{
		HeartbeatInterval:  10 * time.Millisecond,
		SMInterval:         2 * time.Millisecond,
		NakInterval:        2 * time.Millisecond,
		RetransmitLinger:   500 * time.Microsecond,
		HousekeepingPeriod: time.Millisecond,
	}
}
