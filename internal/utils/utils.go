package utils

import "time"

func SleepInSecs(duration int) {
	<-time.After(time.Duration(duration) * time.Second)
}
