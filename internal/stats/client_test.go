package stats

import (
	"bufio"
	"fmt"
	"net"
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

	statsdAdminURL := fmt.Sprintf("%s:%d", statsDHost, adminPort)
	conn, err := connectToStatsD(t, statsdAdminURL)
	if err != nil {
		//failed to connect to statsd endpoint, skipping test.
		t.Logf("TestStatsDClient- Skipping this test.")
		return
	}

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

func connectToStatsD(t *testing.T, statsdAdminURL string) (net.Conn, error) {
	conn, err := net.Dial("tcp", statsdAdminURL)
	if err != nil {
		t.Logf("Unable to connect to StatsD admin endpoint: %s.", statsdAdminURL)
		return nil, err
	}
	return conn, nil
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
