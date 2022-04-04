package app

import (
	"fmt"
	grb "github.com/desertbit/grumble"
	"github.com/vigilglc/raft-lsm/server/api/rpcpb"
	"github.com/vigilglc/raft-lsm/server/backend/kvpb"
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

type RangeExecutor interface {
	Next(N uint64) (kvs []*kvpb.KV, hasMore bool, err error)
	Close() error
}

type ExeGetFunc func(key string, linearizable bool) (val string, err error)
type ExePutFunc func(kvs []*kvpb.KV) (err error)
type ExeDelFunc func(keys []string) (err error)
type ExeRangeFunc func(from, to string, asc, linearizable bool) (exe RangeExecutor, err error)

type ExeAddMemberFunc func(mem *rpcpb.Member) (members []*rpcpb.Member, err error)
type ExePromoteMemberFunc func(ID uint64) (members []*rpcpb.Member, err error)
type ExeRemoveMemberFunc func(ID uint64) (members []*rpcpb.Member, err error)
type ExeClusterStatusFunc func(ID uint64) (status *rpcpb.ClusterStatusResponse, err error)

type ExeRaftStatusFunc func(ID uint64) (status *rpcpb.StatusResponse, err error)
type ExeTransferLeaderFunc func(fromID, toID uint64) (err error)

var (
	rangeExe RangeExecutor = nil
)

var (
	exeGetFunc            ExeGetFunc
	exePutFunc            ExePutFunc
	exeDelFunc            ExeDelFunc
	exeRangeFunc          ExeRangeFunc
	exeAddMemberFunc      ExeAddMemberFunc
	exePromoteMemberFunc  ExePromoteMemberFunc
	exeRemoveMemberFunc   ExeRemoveMemberFunc
	exeClusterStatusFUnc  ExeClusterStatusFunc
	exeRaftStatusFunc     ExeRaftStatusFunc
	exeTransferLeaderFunc ExeTransferLeaderFunc
)

func SetExeGetFunc(fun ExeGetFunc) {
	exeGetFunc = fun
}
func SetExePutFunc(fun ExePutFunc) {
	exePutFunc = fun
}
func SetExeDelFunc(fun ExeDelFunc) {
	exeDelFunc = fun
}
func SetExeRangeFunc(fun ExeRangeFunc) {
	exeRangeFunc = fun
}

func SetExeAddMemberFunc(fun ExeAddMemberFunc) {
	exeAddMemberFunc = fun
}
func SetExePromoteMemberFunc(fun ExePromoteMemberFunc) {
	exePromoteMemberFunc = fun
}
func SetExeRemoveMemberFunc(fun ExeRemoveMemberFunc) {
	exeRemoveMemberFunc = fun
}
func SetExeClusterStatusFunc(fun ExeClusterStatusFunc) {
	exeClusterStatusFUnc = fun
}

func SetExeRaftStatusFunc(fun ExeRaftStatusFunc) {
	exeRaftStatusFunc = fun
}
func SetExeTransferLeaderFunc(fun ExeTransferLeaderFunc) {
	exeTransferLeaderFunc = fun
}

func wrapCmdRunFunc(inner func(c *grb.Context) error) func(c *grb.Context) error {
	return func(c *grb.Context) error {
		switch c.Command.Name {
		case cmdGet.Name:
			fallthrough
		case cmdPut.Name:
			fallthrough
		case cmdDel.Name:
			fallthrough

		case cmdAddMember.Name:
			fallthrough
		case cmdPromoteMember.Name:
			fallthrough
		case cmdRemoveMember.Name:
			fallthrough
		case cmdClusterStatus.Name:
			fallthrough
		case cmdRaftStatus.Name:
			fallthrough
		case cmdTransferLeader.Name:
			fallthrough
		case cmdRangeBegin.Name:
			if rangeExe != nil {
				c.App.PrintError(ErrClientStateInvalid)
				return nil
			}
		case cmdRangeNext.Name:
			fallthrough
		case cmdRangeClose.Name:
			if rangeExe == nil {
				c.App.PrintError(ErrClientStateInvalid)
				return nil
			}
		default:
			c.App.PrintError(ErrUnknownCommand)
			return nil
		}
		return inner(c)
	}
}

const (
	normalPrompt   = ">>>"
	exeRangePrompt = "R>>"
)

var (
	app = grb.New(&grb.Config{
		Name:        "raft-lsm-cli",
		Description: "client for raft-lsm-server",
		Flags: func(f *grb.Flags) {
			f.StringL(flagHost, "", "hosts to connect, e.g. \"127.0.0.1:8080;127.1.1.1:8081\"")
		},
		HistoryLimit: 1000,
		Prompt:       normalPrompt,
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
		Run: wrapCmdRunFunc(func(c *grb.Context) error {
			key := c.Args.String(argKey)
			linearizable := c.Args.Bool(argLinearizable)
			val, err := exeGetFunc(key, linearizable)
			if err != nil {
				c.App.PrintError(err)
				return nil
			}
			_, _ = c.App.Println(val)
			return nil
		}),
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
		Run: wrapCmdRunFunc(func(c *grb.Context) error {
			words := c.Args.StringList(argKV)
			if len(words) == 0 {
				return nil
			}
			if len(words)%2 != 0 {
				c.App.PrintError(ErrPutArgumentsInvalid)
				return nil
			}
			var kvs []*kvpb.KV
			for i := 0; i < len(words); i += 2 {
				kvs = append(kvs, &kvpb.KV{Key: words[i], Val: words[i+1]})
			}
			err := exePutFunc(kvs)
			if err != nil {
				c.App.PrintError(err)
				return nil
			}
			_, _ = c.App.Println("success")
			return nil
		}),
	}
	cmdDel = &grb.Command{
		Name:    "DEL",
		Aliases: []string{"del"},
		Help:    "delete KVs",
		Usage:   "DEL key...",
		Args: func(a *grb.Args) {
			a.StringList(argKey, "KV's keys, e.g. \"key0 key1 key2\"")
		},
		Run: wrapCmdRunFunc(func(c *grb.Context) error {
			keys := c.Args.StringList(argKey)
			err := exeDelFunc(keys)
			if err != nil {
				c.App.PrintError(err)
				return nil
			}
			_, _ = c.App.Println("success")
			return nil
		}),
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
		Run: wrapCmdRunFunc(func(c *grb.Context) error {
			from := c.Args.String(argFrom)
			to := c.Args.String(argTo)
			asc := c.Args.Bool(argAsc)
			linearizable := c.Args.Bool(argLinearizable)
			exe, err := exeRangeFunc(from, to, asc, linearizable)
			if err != nil {
				c.App.PrintError(err)
				return nil
			}
			rangeExe = exe
			c.App.SetPrompt(exeRangePrompt)
			return nil
		}),
	}
	cmdRangeNext = &grb.Command{
		Name:    "NEXT",
		Aliases: []string{"next"},
		Help:    "get next N KVs of the range iterator",
		Usage:   "NEXT N",
		Args: func(a *grb.Args) {
			a.Uint64(argCount, "expected count of KVs got", grb.Default(1))
		},
		Run: wrapCmdRunFunc(func(c *grb.Context) error {
			N := c.Args.Uint64(argCount)
			if N == 0 {
				return nil
			}
			kvs, hasMore, err := rangeExe.Next(N)
			if err != nil {
				c.App.PrintError(err)
				return nil
			}
			if hasMore {
				_, _ = c.App.Printf("range has more KVs")
			} else {
				_, _ = c.App.Printf("no more KVs")
			}
			for _, kv := range kvs {
				_, _ = c.App.Printf("{K: %s, V: %s}", kv.Key, kv.Val)
			}
			if len(kvs) > 0 {
				_, _ = c.App.Println()
			}
			return nil
		}),
	}
	cmdRangeClose = &grb.Command{
		Name:    "CLOSE",
		Aliases: []string{"close"},
		Help:    "close the range iterator",
		Usage:   "CLOSE",
		Args: func(a *grb.Args) {
		},
		Run: wrapCmdRunFunc(func(c *grb.Context) error {
			var exe = rangeExe
			rangeExe = nil
			c.App.SetPrompt(normalPrompt)
			if err := exe.Close(); err != nil {
				c.App.PrintError(err)
			}
			return nil
		}),
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
		Run: wrapCmdRunFunc(func(c *grb.Context) error {
			_ = c.Args.String(argMember) // TODO: convert to rpcpb.Member
			members, err := exeAddMemberFunc(new(rpcpb.Member))
			if err != nil {
				c.App.PrintError(err)
				return nil
			}
			_, _ = c.App.Println("success")
			_, _ = c.App.Println(members) // TODO: print members
			return nil
		}),
	}
	cmdPromoteMember = &grb.Command{
		Name:    "PROMEM",
		Aliases: []string{"promem"},
		Help:    "promote a member",
		Usage:   "PROMEM ID",
		Args: func(a *grb.Args) {
			a.Uint64(argID, "member's node ID")
		},
		Run: wrapCmdRunFunc(func(c *grb.Context) error {
			nodeID := c.Args.Uint64(argID)
			members, err := exePromoteMemberFunc(nodeID)
			if err != nil {
				c.App.PrintError(err)
				return nil
			}
			_, _ = c.App.Println("success")
			_, _ = c.App.Println(members)
			return nil
		}),
	}
	cmdRemoveMember = &grb.Command{
		Name:    "RMVMEM",
		Aliases: []string{"rmvmem"},
		Help:    "remove a member",
		Usage:   "RMVMEM ID",
		Args: func(a *grb.Args) {
			a.Uint64(argID, "member's node ID")
		},
		Run: wrapCmdRunFunc(func(c *grb.Context) error {
			nodeID := c.Args.Uint64(argID)
			members, err := exeRemoveMemberFunc(nodeID)
			if err != nil {
				c.App.PrintError(err)
				return nil
			}
			_, _ = c.App.Println("success")
			_, _ = c.App.Println(members)
			return nil
		}),
	}
	cmdClusterStatus = &grb.Command{
		Name:    "CSTATUS",
		Aliases: []string{"cstatus"},
		Help:    "get cluster status",
		Usage:   "CSTATUS [ID]",
		Args: func(a *grb.Args) {
			a.Uint64(argID, "member's node ID", grb.Default(raft.None))
		},
		Run: wrapCmdRunFunc(func(c *grb.Context) error {
			nodeID := c.Args.Uint64(argID)
			status, err := exeClusterStatusFUnc(nodeID)
			if err != nil {
				c.App.PrintError(err)
				return nil
			}
			_, _ = c.App.Println("success")
			_, _ = c.App.Println(status)
			return nil
		}),
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
		Run: wrapCmdRunFunc(func(c *grb.Context) error {
			nodeID := c.Args.Uint64(argID)
			status, err := exeRaftStatusFunc(nodeID)
			if err != nil {
				c.App.PrintError(err)
				return nil
			}
			_, _ = c.App.Println("success")
			_, _ = c.App.Println(status)
			return nil
		}),
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
		Run: wrapCmdRunFunc(func(c *grb.Context) error {
			fromID := c.Args.Uint64(argFrom)
			toID := c.Args.Uint64(argTo)
			err := exeTransferLeaderFunc(fromID, toID)
			if err != nil {
				c.App.PrintError(err)
				return nil
			}
			_, _ = c.App.Println("success")
			return nil
		}),
	}

	// endregion
)

func init() {
	app.AddCommand(cmdGet)
	app.AddCommand(cmdPut)
	app.AddCommand(cmdDel)

	app.AddCommand(cmdRangeBegin)
	app.AddCommand(cmdRangeNext)
	app.AddCommand(cmdRangeClose)

	app.AddCommand(cmdAddMember)
	app.AddCommand(cmdPromoteMember)
	app.AddCommand(cmdRemoveMember)
	app.AddCommand(cmdClusterStatus)

	app.AddCommand(cmdRaftStatus)
	app.AddCommand(cmdTransferLeader)

	app.OnInit(func(a *grb.App, flags grb.FlagMap) error {
		host := flags.String(flagHost)
		urls := strings.Split(host, ";")
		fmt.Println(urls) // TODO: initiate clients...
		return nil
	})
}

func Run() error {
	return app.Run()
}
