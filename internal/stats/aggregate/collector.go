package aggregate

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/flipkart-incubator/dkv/internal/stats"
)

type StatListener struct {
	/* Time Stamp to Channel Map for Writing Output */
	listenerInfoMap map[string]*ListenerInfo
	/* Mutex for Safe Access */
	mapMutex sync.Mutex
}

type MetricEvent struct {
	metric stats.DKVMetrics
	host   string
}

func NewStatListener() *StatListener {
	return &StatListener{listenerInfoMap: make(map[string]*ListenerInfo)}
}

func (sl *StatListener) Register(host string, outputChannel chan MetricEvent) (int64, error) {
	sl.mapMutex.Lock()
	defer sl.mapMutex.Unlock()
	if _, exists := sl.listenerInfoMap[host]; !exists {
		newListenerInfo := NewListenerInfo(host)
		if err := newListenerInfo.Start(); err != nil {
			return 0, err
		}
		sl.listenerInfoMap[host] = newListenerInfo
	}
	return sl.listenerInfoMap[host].Register(outputChannel), nil
}

func (sl *StatListener) DeRegister(host string, id int64) {
	sl.mapMutex.Lock()
	if li, exists := sl.listenerInfoMap[host]; exists {
		li.DeRegister(id)
		if li.GetChannelCount() == 0 {
			li.Stop()
			delete(sl.listenerInfoMap, host)
		}
	}
	defer sl.mapMutex.Unlock()
}

type ListenerInfo struct {
	host             string
	outputChannelMap map[int64]chan MetricEvent
	inputChannel     <-chan MetricEvent
	mapMutex         sync.Mutex
	ctx              context.Context
	cancelFunc       context.CancelFunc
}

func NewListenerInfo(host string) *ListenerInfo {
	ctx, cancelFunc := context.WithCancel(context.Background())
	return &ListenerInfo{host: host, outputChannelMap: make(map[int64]chan MetricEvent, 2), inputChannel: make(<-chan MetricEvent, 5), ctx: ctx, cancelFunc: cancelFunc}
}

func (li *ListenerInfo) GetChannelCount() int {
	return len(li.outputChannelMap)
}

func (li *ListenerInfo) Start() error {
	var err error
	if li.inputChannel, err = getStreamChannel(li.host, li.ctx); err == nil {
		go li.BroadCast()
	}
	return err
}

func (li *ListenerInfo) BroadCast() {
	for {
		select {
		case e := <-li.inputChannel:
			li.mapMutex.Lock()
			for _, outputChan := range li.outputChannelMap {
				outputChan <- e
			}
			li.mapMutex.Unlock()
		case <-li.ctx.Done():
			li.mapMutex.Lock()
			for _, outputChan := range li.outputChannelMap {
				close(outputChan)
			}
			li.mapMutex.Unlock()
		}
	}
}

func getStreamChannel(host string, ctx context.Context) (<-chan MetricEvent, error) {

	transport := &http.Transport{
		DisableCompression: true,
	}
	client := &http.Client{
		Transport: transport,
	}
	request, err := http.NewRequest("GET", "http://"+host+"/metrics/stream", nil)
	if err != nil {
		return nil, err
	}
	/* Add Header to accept streaming events */
	request.Header.Set("Accept", "text/event-stream")
	/* Make Channel to Report Events */
	eventChannel := make(chan MetricEvent, 5)
	/* Ensure Request gets Cancelled along with Context */
	requestWithContext := request.WithContext(ctx)
	/* Fire Request */
	if response, err := client.Do(requestWithContext); err == nil {
		/* Open a Reader on Response Body */
		go parseEvent(response, eventChannel, host, ctx)
	} else {
		return nil, err
	}
	return eventChannel, nil
}

func parseEvent(response *http.Response, eventChannel chan MetricEvent, host string, ctx context.Context) {
	defer response.Body.Close()
	br := bufio.NewReader(response.Body)
	for {
		select {
		case <-ctx.Done():
			close(eventChannel)
			return
		default:
			/* Read Lines Upto Delimiter */
			if readBytes, err := br.ReadBytes('\n'); err == nil {
				if event, err := buildEvent(readBytes); err == nil {
					eventChannel <- MetricEvent{metric: *event, host: host}
				} else {
					log.Printf("Error in parsing Event: %v \n", err)
				}
			} else {
				log.Printf("Error in reading Stream: %v \n", err)
			}
		}
	}
}

func buildEvent(byts []byte) (*stats.DKVMetrics, error) {
	splits := bytes.Split(byts, []byte{':', ' '})
	if len(splits) == 2 {
		dkvMetrics := &stats.DKVMetrics{}
		err := json.Unmarshal(splits[1], dkvMetrics)
		return dkvMetrics, err
	}
	return nil, errors.New("Invalid Response")
}

func (li *ListenerInfo) Stop() {
	li.cancelFunc()
}

func (li *ListenerInfo) Register(outputChannel chan MetricEvent) int64 {
	channelId := time.Now().UnixNano()
	li.mapMutex.Lock()
	li.outputChannelMap[channelId] = outputChannel
	li.mapMutex.Unlock()
	return channelId
}

func (li *ListenerInfo) DeRegister(id int64) {
	li.mapMutex.Lock()
	if outputChannel, ok := li.outputChannelMap[id]; ok {
		li.unsafeDeregister(outputChannel, id)
	}
	li.mapMutex.Unlock()
}

func (li *ListenerInfo) unsafeDeregister(outputChannel chan MetricEvent, id int64) {
	/* Close Channel */
	close(outputChannel)
	/* Delete current Channel from Broadcast Map */
	delete(li.outputChannelMap, id)
}
