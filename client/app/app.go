package app

import (
	"fmt"
	grb "github.com/desertbit/grumble"
	"go.etcd.io/etcd/raft/v3"
	"math"
	"strings"
)

const (
	flagHost = "host"

	argKey          = "key"
	argLinearizable = "linearizable"
	argKV           = "KV"
	argFrom         = "from"
	argTo           = "to"
	argAsc          = "asc"
	argCount        = "N"
	argID           = "ID"
	argMember       = "member"
)

var (
	App = grb.New(&grb.Config{
		Name:        "raft-lsm-cli",
		Description: "client for raft-lsm-server",
		Flags: func(f *grb.Flags) {
			f.StringL(flagHost, "", "hosts to connect, e.g. \"127.0.0.1:8080;127.1.1.1:8081\"")
		},
		HistoryLimit: 1000,
		Prompt:       ">>>",
	})

	// region kv cmd

	cmdGet = &grb.Command{
		Name:    "GET",
		Aliases: []string{"get"},
		Help:    "get KV's val",
		Usage:   "GET key [linearizable]",
		Args: func(a *grb.Args) {
			a.String(argKey, "KV's key")
			a.Bool(argLinearizable, "whether to do linearizable reads", grb.Default(true))
		},
		Run: func(c *grb.Context) error {
			key := c.Args.String(argKey)
			linearizable := c.Args.Bool(argLinearizable)
			_, _ = c.App.Println(key, linearizable)
			return nil
		},
	}
	cmdPut = &grb.Command{
		Name:    "PUT",
		Aliases: []string{"put"},
		Help:    "put KVs",
		Usage:   "PUT str...",
		Args: func(a *grb.Args) {
			a.StringList(argKV, "KV's keys and vals, e.g. \"key0 val0 key1 val1\"", grb.Min(2),
				grb.Max(math.MaxUint32))
		},
		Run: func(c *grb.Context) error {
			keyVals := c.Args.StringList(argKV)
			_, _ = c.App.Println(keyVals)
			return nil
		},
	}
	cmdDel = &grb.Command{
		Name:    "DEL",
		Aliases: []string{"del"},
		Help:    "delete KVs",
		Usage:   "DEL key...",
		Args: func(a *grb.Args) {
			a.StringList(argKey, "KV's keys, e.g. \"key0 key1 key2\"")
		},
		Run: func(c *grb.Context) error {
			keys := c.Args.StringList(argKey)
			_, _ = c.App.Println(keys)
			return nil
		},
	}
	cmdRangeBegin = &grb.Command{
		Name:    "RANGE",
		Aliases: []string{"range"},
		Help:    "get a KV iterator within the given range",
		Usage:   "RANGE from to [asc] [linearizable]",
		Args: func(a *grb.Args) {
			a.String(argFrom, "KV's key begins from")
			a.String(argTo, "KV's key ends to")
			a.Bool(argAsc, "order of KV's key", grb.Default(true))
			a.Bool(argLinearizable, "whether to do linearizable reads", grb.Default(true))
		},
		Run: func(c *grb.Context) error {
			from := c.Args.String(argFrom)
			to := c.Args.String(argTo)
			asc := c.Args.Bool(argAsc)
			linearizable := c.Args.Bool(argLinearizable)
			_, _ = c.App.Println(from, to, asc, linearizable)
			return nil
		},
	}
	cmdRangeNext = &grb.Command{
		Name:    "NEXT",
		Aliases: []string{"next"},
		Help:    "get next N KVs of the range iterator",
		Usage:   "NEXT N",
		Args: func(a *grb.Args) {
			a.Uint64(argCount, "expected count of KVs got", grb.Default(1))
		},
		Run: func(c *grb.Context) error {
			N := c.Args.Uint64(argCount)
			_, _ = c.App.Println(N)
			return nil
		},
	}
	cmdRangeClose = &grb.Command{
		Name:    "CLOSE",
		Aliases: []string{"close"},
		Help:    "close the range iterator",
		Usage:   "CLOSE",
		Args: func(a *grb.Args) {
		},
		Run: func(c *grb.Context) error {
			_, _ = c.App.Println()
			return nil
		},
	}

	// endregion

	// region cluster cmd

	cmdAddMember = &grb.Command{
		Name:    "ADDMEM",
		Aliases: []string{"addmem"},
		Help:    "add a new member",
		Usage:   "ADDMEM str",
		Args: func(a *grb.Args) {
			a.String(argMember, "member's field sequence, e.g. "+
				"\"name=node2;host=127.0.0.1;raftPort=8080;servicePort=8090;learner=true;\"")
		},
		Run: func(c *grb.Context) error {
			memSeq := c.Args.String(argMember)
			_, _ = c.App.Println(memSeq)
			return nil
		},
	}
	cmdPromoteMember = &grb.Command{
		Name:    "PROMEM",
		Aliases: []string{"promem"},
		Help:    "promote a member",
		Usage:   "PROMEM ID",
		Args: func(a *grb.Args) {
			a.Uint64(argID, "member's node ID")
		},
		Run: func(c *grb.Context) error {
			nodeID := c.Args.Uint64(argID)
			_, _ = c.App.Println(nodeID)
			return nil
		},
	}
	cmdRemoveMember = &grb.Command{
		Name:    "RMVMEM",
		Aliases: []string{"rmvmem"},
		Help:    "remove a member",
		Usage:   "RMVMEM ID",
		Args: func(a *grb.Args) {
			a.Uint64(argID, "member's node ID")
		},
		Run: func(c *grb.Context) error {
			nodeID := c.Args.Uint64(argID)
			_, _ = c.App.Println(nodeID)
			return nil
		},
	}
	cmdClusterStatus = &grb.Command{
		Name:    "CSTATUS",
		Aliases: []string{"cstatus"},
		Help:    "get cluster status",
		Usage:   "CSTATUS [ID]",
		Args: func(a *grb.Args) {
			a.Uint64(argID, "member's node ID", grb.Default(raft.None))
		},
		Run: func(c *grb.Context) error {
			nodeID := c.Args.Uint64(argID)
			_, _ = c.App.Println(nodeID)
			return nil
		},
	}

	// endregion

	// region raft cmd

	cmdRaftStatus = &grb.Command{
		Name:    "RSTATUS",
		Aliases: []string{"rstatus"},
		Help:    "get raft status",
		Usage:   "RSTATUS [ID]",
		Args: func(a *grb.Args) {
			a.Uint64(argID, "member's node ID", grb.Default(raft.None))
		},
		Run: func(c *grb.Context) error {
			nodeID := c.Args.Uint64(argID)
			_, _ = c.App.Println(nodeID)
			return nil
		},
	}

	cmdTransferLeader = &grb.Command{
		Name:    "TRANSFER",
		Aliases: []string{"transfer"},
		Help:    "transfer raft leader",
		Usage:   "TRANSFER from to",
		Args: func(a *grb.Args) {
			a.Uint64(argFrom, "from member node ID")
			a.Uint64(argTo, "to member node ID")
		},
		Run: func(c *grb.Context) error {
			fromID := c.Args.Uint64(argFrom)
			toID := c.Args.Uint64(argTo)
			_, _ = c.App.Println(fromID, toID)
			return nil
		},
	}

	// endregion
)

func init() {
	App.AddCommand(cmdGet)
	App.AddCommand(cmdPut)
	App.AddCommand(cmdDel)

	App.AddCommand(cmdRangeBegin)
	App.AddCommand(cmdRangeNext)
	App.AddCommand(cmdRangeClose)

	App.AddCommand(cmdAddMember)
	App.AddCommand(cmdPromoteMember)
	App.AddCommand(cmdRemoveMember)
	App.AddCommand(cmdClusterStatus)

	App.AddCommand(cmdRaftStatus)
	App.AddCommand(cmdTransferLeader)

	App.OnInit(func(a *grb.App, flags grb.FlagMap) error {
		host := flags.String(flagHost)
		urls := strings.Split(host, ";")
		fmt.Println(urls) // TODO: initiate clients...
		return nil
	})
}
