package stats

import (
	"sync"
	"time"
)

type StatPublisher struct {
	/* Time Stamp to Channel Map for Writing Output */
	outputChannelMap map[int64]chan DKVMetrics
	/* Mutex for Safe Access */
	mapMutex sync.Mutex
}

func NewStatPublisher() *StatPublisher {
	return &StatPublisher{
		outputChannelMap: make(map[int64]chan DKVMetrics, 10),
	}
}

func (sp *StatPublisher) Register(outputChannel chan DKVMetrics) int64 {
	channelId := time.Now().UnixNano()
	sp.mapMutex.Lock()
	sp.outputChannelMap[channelId] = outputChannel
	sp.mapMutex.Unlock()
	return channelId
}

func (sp *StatPublisher) DeRegister(id int64) {
	sp.mapMutex.Lock()
	if outputChannel, ok := sp.outputChannelMap[id]; ok {
		sp.unsafeDeregister(outputChannel, id)
	}
	sp.mapMutex.Unlock()
}

func (sp *StatPublisher) unsafeDeregister(outputChannel chan DKVMetrics, id int64) {
	/* Close Channel */
	close(outputChannel)
	/* Delete current Channel from Broadcast Map */
	delete(sp.outputChannelMap, id)
}

func (sp *StatPublisher) Run() {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ticker.C:
			dkvMetrics, _ := GetMetrics()
			sp.mapMutex.Lock()
			for id, outputChannel := range sp.outputChannelMap {
				select {
				case outputChannel <- *dkvMetrics:
				default:
					sp.unsafeDeregister(outputChannel, id)
				}
			}
			sp.mapMutex.Unlock()
		}
	}
}
