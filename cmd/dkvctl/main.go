package main

import (
	"flag"
	"fmt"
	"strings"

	"github.com/flipkart-incubator/dkv/internal/ctl"
)

const sep = ":"

var (
	dkvAddr string
	setKV   string
	getK    string
)

func init() {
	flag.StringVar(&dkvAddr, "dkvAddr", "127.0.0.1:8080", "DKV server address - host:port")
	flag.StringVar(&setKV, "set", "", fmt.Sprintf("Set key%svalue", sep))
	flag.StringVar(&getK, "get", "", "Get key")
}

func main() {
	flag.Parse()
	client, err := ctl.NewInSecureDKVClient(dkvAddr)
	if err != nil {
		fmt.Printf("Unable to create DKV client. Error: %v\n", err)
	}
	defer client.Close()

	if setKV != "" {
		if kv := strings.Split(setKV, sep); len(kv) != 2 {
			fmt.Printf("Expected in key:value format for set. Given: %s\n", setKV)
		} else {
			if err := client.Put([]byte(kv[0]), []byte(kv[1])); err != nil {
				fmt.Printf("Unable to perform PUT. Error: %v\n", err)
			}
		}
	}

	if getK != "" {
		if res, err := client.Get([]byte(getK)); err != nil {
			fmt.Printf("Unable to perform GET. Error: %v\n", err)
		} else {
			fmt.Println(string(res.Value))
		}
	}
}
