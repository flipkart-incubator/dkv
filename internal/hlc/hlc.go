package hlc

import (
	"github.com/kpango/fastime"
	"time"
)

// Now returns current time
func Now() time.Time {
	return fastime.Now()
}

// UnixNow returns current unix time
func UnixNow() uint64 {
	return uint64(fastime.UnixNow())
}

//InThePast checks if the provided unix time has elapsed.
func InThePast(epoch uint64) bool {
	return epoch > 0 && epoch < UnixNow()
}

// GetTimeAgo returns time difference between current and provided unix time
func GetTimeAgo(epoch uint64) uint64 {
	return UnixNow() - epoch
}

// GetUnixTimeFromNow add delta (in seconds) to current time and return new unix time
func GetUnixTimeFromNow(delta uint64) uint64 {
	return UnixNow() + delta
}
