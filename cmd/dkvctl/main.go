package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/flipkart-incubator/dkv/internal/ctl"
)

type cmd struct {
	name    string
	argDesc string
	cmdDesc string
	fn      func(*cmd, *ctl.DKVClient, ...string)
	value   string
}

var cmds = []*cmd{
	{"set", "<key> <value>", "Set a key value pair", (*cmd).set, ""},
	{"get", "<key>", "Get value for the given key", (*cmd).get, ""},
	{"iter", "[<prefix>]:[<startKey>]", "Iterate through the keyspace", (*cmd).iter, ""},
	{"backup", "<path>", "Backs up data to the given path", (*cmd).backup, ""},
	{"restore", "<path>", "Restores data from the given path", (*cmd).restore, ""},
	{"addNode", "<nodeId> <nodeUrl>", "Add a DKV node to cluster", (*cmd).addNode, ""},
	{"removeNode", "<nodeId", "Remove a DKV node from cluster", (*cmd).removeNode, ""},
}

func (c *cmd) usage() {
	fmt.Printf("  -%s %s - %s\n", c.name, c.argDesc, c.cmdDesc)
}

func (c *cmd) set(client *ctl.DKVClient, args ...string) {
	if len(args) != 2 {
		c.usage()
	} else {
		if err := client.Put([]byte(args[0]), []byte(args[1])); err != nil {
			fmt.Printf("Unable to perform SET. Error: %v\n", err)
		} else {
			fmt.Println("OK")
		}
	}
}

func (c *cmd) get(client *ctl.DKVClient, args ...string) {
	if len(args) != 1 {
		c.usage()
	} else {
		if res, err := client.Get([]byte(args[0])); err != nil {
			fmt.Printf("Unable to perform GET. Error: %v\n", err)
		} else {
			fmt.Println(string(res.Value))
		}
	}
}

func (c *cmd) iter(client *ctl.DKVClient, args ...string) {
	var kyPrfx, strtKy string
	if len(args) > 0 {
		comps := strings.Split(args[0], ":")
		kyPrfx, strtKy = comps[0], comps[0]
		if len(comps) > 1 {
			strtKy = comps[1]
		}
	}
	if ch, err := client.Iterate([]byte(kyPrfx), []byte(strtKy)); err != nil {
		fmt.Printf("Unable to perform iteration. Error: %v\n", err)
	} else {
		for kvp := range ch {
			if kvp.ErrMsg != "" {
				fmt.Printf("Error: %s\n", kvp.ErrMsg)
			} else {
				fmt.Printf("%s => %s\n", kvp.Key, kvp.Val)
			}
		}
	}
}

func (c *cmd) backup(client *ctl.DKVClient, args ...string) {
	if len(args) != 1 {
		c.usage()
	} else {
		if err := client.Backup(args[0]); err != nil {
			fmt.Printf("Unable to perform backup. Error: %v\n", err)
		} else {
			fmt.Println("Successfully backed up")
		}
	}
}

func (c *cmd) restore(client *ctl.DKVClient, args ...string) {
	if len(args) != 1 {
		c.usage()
	} else {
		if err := client.Restore(args[0]); err != nil {
			fmt.Printf("Unable to perform restore. Error: %v\n", err)
		} else {
			fmt.Println("Successfully restored")
		}
	}
}

func (c *cmd) addNode(client *ctl.DKVClient, args ...string) {
	if len(args) != 2 {
		c.usage()
	} else {
		if nodeID, err := strconv.ParseUint(args[0], 10, 32); err != nil {
			fmt.Printf("Unable to convert %s into an unsigned 32-bit integer\n", args[0])
		} else {
			if err := client.AddNode(uint32(nodeID), args[1]); err != nil {
				fmt.Printf("Unable to add node with ID: %d and URL: %s\n", nodeID, args[1])
			}
		}
	}
}

func (c *cmd) removeNode(client *ctl.DKVClient, args ...string) {
	if len(args) != 1 {
		c.usage()
	} else {
		if nodeID, err := strconv.ParseUint(args[0], 10, 32); err != nil {
			fmt.Printf("Unable to convert %s into an unsigned 32-bit integer\n", args[0])
		} else {
			if err := client.RemoveNode(uint32(nodeID)); err != nil {
				fmt.Printf("Unable to remove node with ID: %d\n", nodeID)
			}
		}
	}
}

var dkvAddr string

func init() {
	flag.StringVar(&dkvAddr, "dkvAddr", "127.0.0.1:8080", "<host>:<port> - DKV server address")
	for _, c := range cmds {
		flag.StringVar(&c.value, c.name, c.value, c.cmdDesc)
	}
	flag.Usage = usage
}

func usage() {
	fmt.Printf("Usage of %s:\n", os.Args[0])
	dkvAddrFlag := flag.Lookup("dkvAddr")
	fmt.Printf("  -dkvAddr %s (default: %s)\n", dkvAddrFlag.Usage, dkvAddrFlag.DefValue)
	for _, cmd := range cmds {
		cmd.usage()
	}
}

func main() {
	if len(os.Args) < 2 {
		usage()
		return
	}

	flag.Parse()
	fmt.Printf("Connecting to DKV service at %s...", dkvAddr)
	client, err := ctl.NewInSecureDKVClient(dkvAddr)
	if err != nil {
		fmt.Printf("\nUnable to create DKV client. Error: %v\n", err)
		return
	}
	fmt.Println("DONE")
	defer client.Close()

	var validCmd bool
	for _, c := range cmds {
		if c.value != "" {
			args := []string{c.value}
			args = append(args, flag.Args()...)
			c.fn(c, client, args...)
			validCmd = true
			break
		}
	}
	if !validCmd {
		usage()
	}
}
