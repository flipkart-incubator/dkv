package main

import (
	"flag"
	"fmt"
	"strings"

	"github.com/flipkart-incubator/dkv/internal/ctl"
)

const sep = ":"

type cmd struct {
	name  string
	desc  string
	fn    func(*ctl.DKVClient, string)
	value string
}

var cmds = []*cmd{
	{"set", fmt.Sprintf("Set key%svalue", sep), set, ""},
	{"get", "Get key", get, ""},
	{"backup", "Backs up data to the given path", backup, ""},
	{"restore", "Restores data from the given path", restore, ""},
}

func set(client *ctl.DKVClient, setKV string) {
	if kv := strings.Split(setKV, sep); len(kv) != 2 {
		fmt.Printf("Expected in key:value format for set. Given: %s\n", setKV)
	} else {
		if err := client.Put([]byte(kv[0]), []byte(kv[1])); err != nil {
			fmt.Printf("Unable to perform SET. Error: %v\n", err)
		}
	}
}

func get(client *ctl.DKVClient, getK string) {
	if res, err := client.Get([]byte(getK)); err != nil {
		fmt.Printf("Unable to perform GET. Error: %v\n", err)
	} else {
		fmt.Println(string(res.Value))
	}
}

func backup(client *ctl.DKVClient, path string) {
	if err := client.Backup(path); err != nil {
		fmt.Printf("Unable to perform backup. Error: %v\n", err)
	} else {
		fmt.Println("Successfully backed up")
	}
}

func restore(client *ctl.DKVClient, path string) {
	if err := client.Restore(path); err != nil {
		fmt.Printf("Unable to perform restore. Error: %v\n", err)
	} else {
		fmt.Println("Successfully restored")
	}
}

var dkvAddr string

func init() {
	flag.StringVar(&dkvAddr, "dkvAddr", "127.0.0.1:8080", "DKV server address - host:port")
	for _, c := range cmds {
		flag.StringVar(&c.value, c.name, c.value, c.desc)
	}
}

func main() {
	flag.Parse()
	client, err := ctl.NewInSecureDKVClient(dkvAddr)
	if err != nil {
		fmt.Printf("Unable to create DKV client. Error: %v\n", err)
	}
	defer client.Close()

	for _, c := range cmds {
		if c.value != "" {
			c.fn(client, c.value)
			break
		}
	}
}
