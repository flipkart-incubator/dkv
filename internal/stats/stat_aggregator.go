package stats

import (
	"context"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
	"sync"
	"time"
)

var (
	MAX_STAT_BUFFER = 5
)

type StatAggregatorRegistry struct {
	statsListener *StatListener
	statAggregatorMap map[int64]*StatAggregator
	/* Mutex for Safe Access */
	mapMutex sync.Mutex
}

type MetricTag interface {
	GetTag(region *serverpb.RegionInfo) string
}

func NewStatAggregatorRegistry() *StatAggregatorRegistry {
	statsListener := NewStatListener()
	return &StatAggregatorRegistry{statsListener: statsListener}
}

func (sr *StatAggregatorRegistry)Register(regions []*serverpb.RegionInfo, tagger func(*serverpb.RegionInfo) string, outputChannel chan map[string]DKVMetrics) int64 {
	hostMap := map[string]string{}
	for _ , region := range regions {
		hostMap[region.GetMasterHost()] =  tagger(region)
	}
	id := time.Now().UnixNano()
	statAggregator := NewStatAggregator(outputChannel,hostMap)
	go statAggregator.Start(sr.statsListener)

	sr.mapMutex.Lock()
	defer sr.mapMutex.Unlock()

	sr.statAggregatorMap[id] = statAggregator
	return id
}

func (sr *StatAggregatorRegistry)DeRegister(id int64) {
	sr.mapMutex.Lock()
	defer sr.mapMutex.Unlock()
	if statAggregator,exist := sr.statAggregatorMap[id]; exist {
		go statAggregator.Stop()
		delete(sr.statAggregatorMap, id)
	}
}

type StatAggregator struct {
	outputChannel chan map[string]DKVMetrics
	aggregatedStatMap map[int64]map[string]DKVMetrics
	hostMap map[string]string
	channelIds map[string]int64
	ctx context.Context
}

func NewStatAggregator(outputChannel chan map[string]DKVMetrics,hostMap map[string]string) *StatAggregator {
	return &StatAggregator{outputChannel: outputChannel,hostMap: hostMap,ctx : context.Background()}
}

func (sa *StatAggregator) Start(listener *StatListener) {
	sa.aggregatedStatMap = make(map[int64]map[string]DKVMetrics,MAX_STAT_BUFFER)
	aggregatedEventChannel := make(chan MetricEvent, 10)
	for host,_ := range sa.hostMap {
		channelId, _ := listener.Register(host,aggregatedEventChannel)
		sa.channelIds[host] = channelId
	}
	for {
		select {
			case event := <-aggregatedEventChannel:
				tag := sa.hostMap[event.host]
				metric := event.metric

				/* ensuring that we have upper buffer size of 5 sec */
				if _ , exist := sa.aggregatedStatMap[metric.TimeStamp]; !exist {
					if len(sa.aggregatedStatMap) > MAX_STAT_BUFFER {
						index := getMinIndex(sa.aggregatedStatMap)
						sa.outputChannel <- sa.aggregatedStatMap[index]
						delete(sa.aggregatedStatMap,index)
					}
					sa.aggregatedStatMap[metric.TimeStamp] = make(map[string]DKVMetrics)
				}

				/* merging metrics*/
				if _ , exist := sa.aggregatedStatMap[metric.TimeStamp][tag] ; !exist {
					metric.Count = 1
					sa.aggregatedStatMap[metric.TimeStamp][tag] = metric
				} else {
					sa.aggregatedStatMap[metric.TimeStamp][tag].Merge(metric)
				}

				/* flushing when all metrics are aggregated */
				if getStatCount(sa.aggregatedStatMap[metric.TimeStamp]) == len(sa.hostMap) {
					sa.outputChannel <- sa.aggregatedStatMap[metric.TimeStamp]
					delete(sa.aggregatedStatMap,metric.TimeStamp)
				}

			case <-sa.ctx.Done():
				for host, channelId := range sa.channelIds {
					listener.DeRegister(host,channelId)
				}
				close(sa.outputChannel)
		}
	}
}

func (sa *StatAggregator) Stop() {
	sa.ctx.Done()
}

func getStatCount(metricMap map[string]DKVMetrics) int {
	count := 0
	for _ , metric :=  range metricMap {
		count += int(metric.Count)
	}
	return count
}

func getMinIndex(m map[int64]map[string]DKVMetrics) int64 {
	var min int64
	for index , _ := range m {
		if min == 0 || min > index {
			min = index
		}
	}
	return min
}


