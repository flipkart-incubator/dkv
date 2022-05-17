package main

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/flipkart-incubator/dkv/pkg/ctl"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"

	flag "github.com/spf13/pflag"
)

type cmd struct {
	name       string
	shorthand  string
	argDesc    string
	cmdDesc    string
	fn         func(*cmd, *ctl.DKVClient, ...string)
	value      string
	emptyValue bool
}

var cmds = []*cmd{
	{"set", "s", "<key> <value>", "Set a key value pair", (*cmd).set, "", false},
	{"del", "d", "<key>", "Delete the given key", (*cmd).del, "", false},
	{"get", "g", "<key>", "Get value for the given key", (*cmd).get, "", false},
	{"iter", "i", "\"*\" | <prefix> [<startKey>]", "Iterate keys matching the <prefix>, starting with <startKey> or \"*\" for all keys", (*cmd).iter, "", false},
	{"keys", "k", "\"*\" | <prefix> [<startKey>]", "Get keys matching the <prefix>, starting with <startKey> or \"*\" for all keys", (*cmd).keys, "", false},
	{"backup", "", "<path>", "Backs up data to the given path", (*cmd).backup, "", false},
	{"restore", "", "<path>", "Restores data from the given path", (*cmd).restore, "", false},
	{"addNode", "", "<nexusUrl>", "Add another master node to DKV cluster", (*cmd).addNode, "", false},
	{"removeNode", "", "<nexusUrl>", "Remove a master node from DKV cluster", (*cmd).removeNode, "", false},
	{"listNodes", "", "", "Lists the various DKV nodes that are part of the Nexus cluster", (*cmd).listNodes, "", true},
	{"clusterInfo", "", "[dcId] [database] [vBucket]", "Lists the various members of the cluster", (*cmd).clusterInfo, "", true},
}

func (c *cmd) usage() {
	fmt.Fprintf(os.Stderr, "Error: Invalid Syntax. Usage of %s:\n", os.Args[0])
	line := ""
	if c.shorthand != "" {
		line = fmt.Sprintf("  -%s, --%s", c.shorthand, c.name)
	} else {
		line = fmt.Sprintf("      --%s", c.name)
	}
	if c.argDesc != "" {
		line += fmt.Sprintf(" %s \t\t%s", c.argDesc, c.cmdDesc)
	}
	fmt.Fprintf(os.Stderr, "%s\n", line)
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

func (c *cmd) del(client *ctl.DKVClient, args ...string) {
	if len(args) != 1 {
		c.usage()
	} else {
		if err := client.Delete([]byte(args[0])); err != nil {
			fmt.Printf("Unable to perform DEL. Error: %v\n", err)
		} else {
			fmt.Println("OK")
		}
	}
}

func (c *cmd) get(client *ctl.DKVClient, args ...string) {
	if len(args) != 1 {
		c.usage()
	} else {
		rc := serverpb.ReadConsistency_LINEARIZABLE
		if res, err := client.Get(rc, []byte(args[0])); err != nil {
			fmt.Printf("Unable to perform GET. Error: %v\n", err)
		} else {
			fmt.Println(string(res.Value))
		}
	}
}

func (c *cmd) keys(client *ctl.DKVClient, args ...string) {
	strtKy, kyPrfx := "", ""
	switch {
	case len(args) == 1:
		if strings.TrimSpace(args[0]) != "*" {
			kyPrfx = args[0]
		}
	case len(args) == 2:
		kyPrfx, strtKy = args[0], args[1]
	}
	if ch, err := client.Iterate([]byte(kyPrfx), []byte(strtKy)); err != nil {
		fmt.Printf("Unable to perform iteration. Error: %v\n", err)
	} else {
		for kvp := range ch {
			if kvp.ErrMsg != "" {
				fmt.Printf("Error: %s\n", kvp.ErrMsg)
			} else {
				fmt.Printf("%s\n", kvp.Key)
			}
		}
	}
}

func (c *cmd) iter(client *ctl.DKVClient, args ...string) {
	strtKy, kyPrfx := "", ""
	switch {
	case len(args) == 1:
		if strings.TrimSpace(args[0]) != "*" {
			kyPrfx = args[0]
		}
	case len(args) == 2:
		kyPrfx, strtKy = args[0], args[1]
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
	if len(args) != 1 {
		c.usage()
	} else {
		nodeURL := args[0]
		if err := client.AddNode(nodeURL); err != nil {
			fmt.Printf("Unable to add node with URL: %s. Error: %v\n", nodeURL, err)
		}
	}
}

func (c *cmd) removeNode(client *ctl.DKVClient, args ...string) {
	if len(args) != 1 {
		c.usage()
	} else {
		nodeURL := args[0]
		if err := client.RemoveNode(nodeURL); err != nil {
			fmt.Printf("Unable to remove node with URL: %s. Error: %v\n", nodeURL, err)
		}
	}
}

func (c *cmd) listNodes(client *ctl.DKVClient, args ...string) {
	if leaderId, members, err := client.ListNodes(); err != nil {
		fmt.Printf("Unable to retrieve the nodes of DKV cluster. Error: %v\n", err)
	} else {
		var ids []uint64
		for id := range members {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		if _, present := members[leaderId]; present {
			fmt.Println("Current DKV cluster members:")
		} else {
			fmt.Println("WARNING: DKV Cluster unhealthy, leader unknown")
			fmt.Println("Current DKV cluster members:")
		}
		for _, id := range ids {
			fmt.Printf("%x => %s (%s) \n", id, members[id].NodeUrl, members[id].Status)
		}
	}
}

func (c *cmd) clusterInfo(client *ctl.DKVClient, args ...string) {
	dcId := ""
	database := ""
	vBucket := ""
	if len(args) > 0 {
		dcId = args[0]
	}
	if len(args) > 1 {
		database = args[1]
	}
	if len(args) > 2 {
		vBucket = args[2]
	}
	vBuckets, err := client.GetClusterInfo(dcId, database, vBucket)
	if err != nil {
		fmt.Printf("Unable to get Status: Error: %v\n", err)
	} else {
		if len(vBuckets) == 0 {
			fmt.Println("Found no nodes with the provided filters")
		} else {
			fmt.Println("Current DKV cluster nodes:")
			for _, bucket := range vBuckets {
				fmt.Println(bucket.String())
			}
		}
	}
}

var dkvAddr, dkvAuthority string

func init() {
	flag.StringVarP(&dkvAddr, "dkvAddr", "a", "127.0.0.1:8080", "<host>:<port> - DKV server address")
	flag.StringVar(&dkvAuthority, "authority", "", "Override :authority pseudo header for routing purposes. Useful while accessing DKV via service mesh.")
	for _, c := range cmds {
		if c.argDesc == "" {
			flag.BoolVar(&c.emptyValue, c.name, c.emptyValue, c.cmdDesc)
		} else {
			if c.shorthand == "" {
				flag.StringVar(&c.value, c.name, c.value, fmt.Sprintf("%s \x00 \x00 %s", c.argDesc, c.cmdDesc))
			} else {
				flag.StringVarP(&c.value, c.name, c.shorthand, c.value, fmt.Sprintf("%s \x00 \x00 %s", c.argDesc, c.cmdDesc))
			}
		}
	}
	flag.CommandLine.SortFlags = false
}

func isFlagPassed(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

func main() {
	if len(os.Args) < 2 {
		flag.Usage()
		return
	}

	flag.Parse()
	fmt.Printf("Connecting to DKV service at %s", dkvAddr)
	if dkvAuthority = strings.TrimSpace(dkvAuthority); dkvAuthority != "" {
		fmt.Printf(" (:authority = %s)", dkvAuthority)
	}
	fmt.Printf("...")
	client, err := ctl.NewInSecureDKVClient(dkvAddr, dkvAuthority, ctl.DefaultConnectOpts)
	if err != nil {
		fmt.Printf("\nUnable to create DKV client. Error: %v\n", err)
		return
	}
	fmt.Println("DONE")
	defer client.Close()

	var validCmd bool
	for _, c := range cmds {
		if !isFlagPassed(c.name) {
			continue
		}
		if c.value != "" || c.emptyValue {
			args := []string{c.value}
			args = append(args, flag.Args()...)
			c.fn(c, client, args...)
			validCmd = true
			break
		}
	}
	if !validCmd {
		flag.Usage()
	}
}
