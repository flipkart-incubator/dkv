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
func UnixNow() int64 {
	return fastime.UnixNow()
}
