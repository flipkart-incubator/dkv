package hlc

import (
	"testing"
)

func TestGetTimeAgo(t *testing.T) {
	now := UnixNow()
	threeSecondsLater := now - 3
	timeAgo := GetTimeAgo(threeSecondsLater)
	if timeAgo != 3 {
		t.Errorf("Time ago incorrect, Expected Value: %d, Actual Value: %d\"", 3, timeAgo)
	}
}

func TestGetUnixTimeFromNow(t *testing.T) {
	time1 := GetUnixTimeFromNow(3)
	now := UnixNow() + 3
	if time1 != now {
		t.Errorf("Time from now incorrect, Expected Value: %d, Actual Value: %d\"", time1, now)
	}
}
