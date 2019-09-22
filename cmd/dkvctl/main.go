package main

import (
	"log"

	"github.com/flipkart-incubator/dkv/internal/ctl"
)

const dkvAddr = "localhost:8080"

func main() {
	client, err := ctl.NewInSecureDKVClient(dkvAddr)
	if err != nil {
		log.Fatalf("Unable to create DKV client. Error: %v", err)
	}
	defer client.Close()

	if err := client.Put([]byte("aKey"), []byte("aValue")); err != nil {
		log.Fatalf("Unable to perform PUT. Error: %v", err)
	}

	if res, err := client.Get([]byte("aKey")); err != nil {
		log.Fatalf("Unable to perform GET. Error: %v", err)
	} else {
		log.Println(string(res))
	}
}
