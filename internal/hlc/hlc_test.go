package hlc

import (
	"github.com/flipkart-incubator/dkv/internal/utils"
	"testing"
)

func TestGetTimeAgo(t *testing.T) {
	time := UnixNow()
	utils.SleepInSecs(3)
	timeAgo := GetTimeAgo(time)
	if (timeAgo != 3) {
		t.Errorf("Time ago incorrect, Expected Value: %d, Actual Value: %d\"", timeAgo, 3)
	}
}

func TestGetUnixTimeFromNow(t *testing.T) {
	time := GetUnixTimeFromNow(3)
	utils.SleepInSecs(3)
	now := UnixNow()
	if (time != now) {
		t.Errorf("Time from now incorrect, Expected Value: %d, Actual Value: %d\"", time, now)
	}
}
