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
