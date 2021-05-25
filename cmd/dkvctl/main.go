package main

import (
	"flag"
	"fmt"
	utils "github.com/flipkart-incubator/dkv/internal"
	"os"
	"sort"
	"strings"

	"github.com/flipkart-incubator/dkv/pkg/ctl"
	"github.com/flipkart-incubator/dkv/pkg/serverpb"
)

type cmd struct {
	name       string
	argDesc    string
	cmdDesc    string
	fn         func(*cmd, *ctl.DKVClient, ...string)
	value      string
	emptyValue bool
}

var cmds = []*cmd{
	{"set", "<key> <value>", "Set a key value pair", (*cmd).set, "", false},
	{"del", "<key>", "Delete the given key", (*cmd).del, "", false},
	{"get", "<key>", "Get value for the given key", (*cmd).get, "", false},
	{"iter", "\"*\" | <prefix> [<startKey>]", "Iterate keys matching the <prefix>, starting with <startKey> or \"*\" for all keys", (*cmd).iter, "", false},
	{"backup", "<path>", "Backs up data to the given path", (*cmd).backup, "", false},
	{"restore", "<path>", "Restores data from the given path", (*cmd).restore, "", false},
	{"addNode", "<nexusUrl>", "Add another master node to DKV cluster", (*cmd).addNode, "", false},
	{"removeNode", "<nexusUrl>", "Remove a master node from DKV cluster", (*cmd).removeNode, "", false},
	{"listNodes", "", "Lists the various DKV nodes that are part of the Nexus cluster", (*cmd).listNodes, "", false},
}

func (c *cmd) usage() {
	if c.argDesc == "" {
		fmt.Printf("  -%s - %s\n", c.name, c.cmdDesc)
	} else {
		fmt.Printf("  -%s %s - %s\n", c.name, c.argDesc, c.cmdDesc)
	}
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
	if leader, nodes, err := client.ListNodes(); err != nil {
		fmt.Printf("Unable to retrieve the nodes of DKV cluster. Error: %v\n", err)
	} else {
		var ids []uint64
		for id := range nodes {
			if id != leader {
				ids = append(ids, id)
			}
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		if leaderURL, present := nodes[leader]; present {
			fmt.Println("Current DKV cluster members:")
			fmt.Printf("%x => %s (leader)\n", leader, leaderURL)
		} else {
			fmt.Println("WARNING: DKV cluster unhealthy, leader unknown")
			fmt.Println("Current cluster members:")
		}
		for _, id := range ids {
			fmt.Printf("%x => %s\n", id, nodes[id])
		}
	}
}

var (
	dkvAddr      string
	caCertPath   string
	dkvAuthority string
)

func init() {
	flag.StringVar(&dkvAddr, "dkvAddr", "127.0.0.1:8080", "<host>:<port> - DKV server address")
	flag.StringVar(&caCertPath, "caCertPath", "", "Path for root certificate of the chain, i.e. CA certificate")
	flag.StringVar(&dkvAuthority, "authority", "", "Override :authority pseudo header for routing purposes. Useful while accessing DKV via service mesh.")

	for _, c := range cmds {
		if c.argDesc == "" {
			flag.BoolVar(&c.emptyValue, c.name, c.emptyValue, c.cmdDesc)
		} else {
			flag.StringVar(&c.value, c.name, c.value, c.cmdDesc)
		}
	}
	flag.Usage = usage
}

func usage() {
	fmt.Printf("Usage of %s:\n", os.Args[0])
	printUsage([]string{"dkvAddr", "authority", "caCertPath"})
	for _, cmd := range cmds {
		cmd.usage()
	}
}

func printUsage(flags []string) {
	for _, flagName := range flags {
		dkvFlag := flag.Lookup(flagName)
		if dkvFlag != nil {
			fmt.Printf("  -%s %s (default: %s)\n", dkvFlag.Name, dkvFlag.Usage, dkvFlag.DefValue)
		}
	}
}

func trimLower(str string) string {
	return strings.ToLower(strings.TrimSpace(str))
}

func main() {
	if len(os.Args) < 2 {
		usage()
		return
	}

	flag.Parse()
	fmt.Printf("Connecting to DKV service at %s", dkvAddr)
	if dkvAuthority = strings.TrimSpace(dkvAuthority); dkvAuthority != "" {
		fmt.Printf(" (:authority = %s)", dkvAuthority)
	}
	fmt.Printf("...")
	client, err := utils.NewDKVClient(utils.DKVConfig{
		SrvrAddr: dkvAddr, CaCertPath: caCertPath}, dkvAuthority)
	if err != nil {
		fmt.Printf("\nUnable to create DKV client. Error: %v\n", err)
		return
	}
	fmt.Println("DONE")
	defer client.Close()

	var validCmd bool
	for _, c := range cmds {
		if c.value != "" || c.emptyValue {
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