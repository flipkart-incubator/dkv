package bench

import (
	"fmt"

	"github.com/flipkart-incubator/dkv/pkg/serverpb"
)

type GetHotKeysBenchmark struct{}

func (this *GetHotKeysBenchmark) ApiName() string {
	return "dkv.serverpb.DKV.Get"
}

func (this *GetHotKeysBenchmark) CreateRequests(numRequests uint) interface{} {
	var getReqs []*serverpb.GetRequest
	for i, j := 0, 0; i < int(numRequests); i, j = i+1, (j+1)%int(numHotKeys) {
		key := []byte(fmt.Sprintf("ExistingKey%d", j))
		getReqs = append(getReqs, &serverpb.GetRequest{key})
	}
	return getReqs
}
