package stats

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	serverPort          = 8125
	adminPort           = 8126
	statsDHost          = "127.0.0.1"
	flushIntervalPrefix = "flushInterval:"
)

func TestStatsDClient(t *testing.T) {
	addr := fmt.Sprintf("%s:%d", statsDHost, serverPort)
	tags := []Tag{NewTag("Tag1", "1"), NewTag("Tag2", "2"), NewTag("Tag3", "3")}
	statsCli := NewStatsDClient(addr, "nexus_test", tags...)
	defer statsCli.Close()

	statsdAdminUrl := fmt.Sprintf("%s:%d", statsDHost, adminPort)
	conn := connectToStatsD(t, statsdAdminUrl)

	s, w := bufio.NewScanner(conn), bufio.NewWriter(conn)
	//flushDuration := statsdFlushInterval(s, w)
	flushDuration := time.Duration(500) * time.Millisecond

	statsCli.GaugeDelta("sample.gauge", 42)
	<-time.After(flushDuration)
	sendCommand("gauges", w)
	logOutput(t, s)

	statsCli.GaugeDelta("sample.gauge", -42)
	<-time.After(flushDuration)
	sendCommand("gauges", w)
	logOutput(t, s)

	statsCli.GaugeDelta("sample.gauge", 42)
	<-time.After(flushDuration)
	sendCommand("gauges", w)
	logOutput(t, s)

	statsCli.Incr("sample.counter", 5)
	<-time.After(flushDuration)
	sendCommand("counters", w)
	logOutput(t, s)

	timing(statsCli)
	<-time.After(flushDuration)
	sendCommand("timers", w)
	logOutput(t, s)
}

func timing(statsCli Client) {
	defer statsCli.Timing("sample.timing", time.Now())
	<-time.After(10 * time.Millisecond)
}

func statsdFlushInterval(s *bufio.Scanner, w *bufio.Writer) time.Duration {
	sendCommand("config", w)
	for s.Scan() {
		msg := s.Text()
		if strings.HasPrefix(msg, flushIntervalPrefix) {
			idx := strings.IndexRune(msg, ':') + 2
			flushDuration, _ := strconv.Atoi(msg[idx:])
			return time.Duration(flushDuration) * time.Millisecond
		}
	}
	return 10 * time.Millisecond
}

func connectToStatsD(t *testing.T, statsdAdminUrl string) net.Conn {
	conn, err := net.Dial("tcp", statsdAdminUrl)
	if err != nil {
		t.Logf("Unable to connect to StatsD admin endpoint: %s. Skipping this test.", statsdAdminUrl)
		os.Exit(0)
	}
	return conn
}

func sendCommand(cmd string, w *bufio.Writer) {
	fmt.Fprint(w, cmd)
	w.Flush()
}

func logOutput(t *testing.T, scanner *bufio.Scanner) {
	for scanner.Scan() {
		msg := scanner.Text()
		t.Logf(msg)
		if msg == "END" {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		t.Error(err)
	}
}
