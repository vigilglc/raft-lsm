package app

import (
	"fmt"
	grb "github.com/desertbit/grumble"
	"github.com/vigilglc/raft-lsm/server/api/rpcpb"
	"github.com/vigilglc/raft-lsm/server/backend/kvpb"
	"math"
	"strconv"
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

const (
	randomSelectedID = "random selected ID"
)

type RangeExecutor interface {
	Begin(from, to string, asc, linearizable bool) error
	Next(N uint64) (kvs []*kvpb.KV, hasMore bool, err error)
	Close() error
}

type InitFunc func(hosts []string) error
type CloseFunc func() error

type ExeGetFunc func(key string, linearizable bool, ID *uint64) (val string, err error)
type ExePutFunc func(kvs []*kvpb.KV) (err error)
type ExeDelFunc func(keys []string) (err error)
type ExeRangeFunc func(from, to string, asc, linearizable bool, ID *uint64) (exe RangeExecutor, err error)

type ExeAddMemberFunc func(mem *rpcpb.Member) (members []*rpcpb.Member, err error)
type ExePromoteMemberFunc func(ID uint64) (members []*rpcpb.Member, err error)
type ExeRemoveMemberFunc func(ID uint64) (members []*rpcpb.Member, err error)
type ExeClusterStatusFunc func(linearizable bool, ID *uint64) (status *rpcpb.ClusterStatusResponse, err error)

type ExeRaftStatusFunc func(linearizable bool, ID *uint64) (status *rpcpb.StatusResponse, err error)
type ExeTransferLeaderFunc func(fromID, toID uint64) (err error)

type ExeHostsFunc func() (hosts []string, err error)
type ExeIDsFunc func() (IDs []uint64, err error)
type ExeResolveFunc func() error

var (
	rangeExe RangeExecutor = nil
)

var (
	appInit               InitFunc  = func(hosts []string) error { return nil }
	closer                CloseFunc = func() error { return nil }
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
	exeIDsFunc            ExeIDsFunc
	exeHostsFunc          ExeHostsFunc
	exeResolveFunc        ExeResolveFunc
)

func SetInitFunc(fun InitFunc) {
	appInit = fun
}
func SetCloseFunc(fun CloseFunc) {
	closer = fun
}

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

func SetExeIDsFunc(fun ExeIDsFunc) {
	exeIDsFunc = fun
}
func SetExeHostsFunc(fun ExeHostsFunc) {
	exeHostsFunc = fun
}
func SetExeResolveFunc(fun ExeResolveFunc) {
	exeResolveFunc = fun
}

const (
	normalPrompt   = ">>> "
	exeRangePrompt = "R>> "
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
		Usage:   "GET key [linearizable] [ID]",
		Args: func(a *grb.Args) {
			a.String(argKey, "KV's key")
			a.Bool(argLinearizable, "whether to do linearizable reads", grb.Default(true))
			a.String(argID, "assigned node ID", grb.Default(randomSelectedID))
		},
		Run: func(c *grb.Context) error {
			key := c.Args.String(argKey)
			linearizable := c.Args.Bool(argLinearizable)
			var ID *uint64
			sID := c.Args.String(argID)
			if sID != randomSelectedID {
				_ID, err := strconv.ParseUint(sID, 10, 64)
				if err != nil {
					c.App.PrintError(err)
					return nil
				}
				ID = &_ID
			}
			val, err := exeGetFunc(key, linearizable, ID)
			if err != nil {
				c.App.PrintError(err)
				return nil
			}
			_, _ = c.App.Println(val)
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
			err := exeDelFunc(keys)
			if err != nil {
				c.App.PrintError(err)
				return nil
			}
			_, _ = c.App.Println("success")
			return nil
		},
	}
	cmdRangeBegin = &grb.Command{
		Name:    "RANGE",
		Aliases: []string{"range"},
		Help:    "get a KV iterator within the given range",
		Usage:   "RANGE from to [asc] [linearizable] [ID]",
		Args: func(a *grb.Args) {
			a.String(argFrom, "KV's key begins from")
			a.String(argTo, "KV's key ends to")
			a.Bool(argAsc, "order of KV's key", grb.Default(true))
			a.Bool(argLinearizable, "whether to do linearizable reads", grb.Default(true))
			a.String(argID, "assigned node ID", grb.Default(randomSelectedID))
		},
		Run: func(c *grb.Context) error {
			from := c.Args.String(argFrom)
			to := c.Args.String(argTo)
			asc := c.Args.Bool(argAsc)
			linearizable := c.Args.Bool(argLinearizable)
			var ID *uint64
			sID := c.Args.String(argID)
			if sID != randomSelectedID {
				_ID, err := strconv.ParseUint(sID, 10, 64)
				if err != nil {
					c.App.PrintError(err)
					return nil
				}
				ID = &_ID
			}
			exe, err := exeRangeFunc(from, to, asc, linearizable, ID)
			if err != nil {
				c.App.PrintError(err)
				return nil
			}
			rangeExe = exe
			c.App.SetPrompt(exeRangePrompt)
			return nil
		},
	}
	cmdRangeNext = &grb.Command{
		Name:    "NEXT",
		Aliases: []string{"next"},
		Help:    "get next N KVs of the range iterator",
		Usage:   "NEXT [N]",
		Args: func(a *grb.Args) {
			a.Uint64(argCount, "expected count of KVs got", grb.Default(uint64(1))) // explicit casting!
		},
		Run: func(c *grb.Context) error {
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
				_, _ = c.App.Printf("range has more KVs: \n")
			} else {
				_, _ = c.App.Printf("no more KVs. \n")
			}
			for _, kv := range kvs {
				_, _ = c.App.Printf("{K: %s, V: %s} ", kv.Key, kv.Val)
			}
			if len(kvs) > 0 {
				_, _ = c.App.Println()
			}
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
			var exe = rangeExe
			rangeExe = nil
			c.App.SetPrompt(normalPrompt)
			if err := exe.Close(); err != nil {
				c.App.PrintError(err)
			}
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
			a.String(argMember, "member's field sequence, isLearner default false, e.g. "+
				"\"name=node2;host=127.0.0.1;raftPort=8080;servicePort=8090;isLearner=true;\"")
		},
		Run: func(c *grb.Context) error {
			mem := new(rpcpb.Member)
			seqs := strings.Split(c.Args.String(argMember), ";")
			fieldSet := map[string]bool{}
			var err error
			var fields int
			var port uint64
			for _, seq := range seqs {
				seq = strings.TrimSpace(seq)
				kv := strings.Split(seq, "=")
				if len(kv) != 2 {
					continue
				}
				key, val := strings.TrimSpace(kv[0]), strings.TrimSpace(kv[1])
				if fieldSet[key] {
					c.App.PrintError(ErrAddMemberArgumentsInvalid)
					return nil
				}
				fieldSet[key] = true
				switch key {
				case "name":
					mem.Name = val
					fields++
				case "host":
					mem.Host = val
					fields++
				case "raftPort":
					port, err = strconv.ParseUint(val, 10, 32)
					mem.RaftPort = uint32(port)
					fields++
				case "servicePort":
					port, err = strconv.ParseUint(val, 10, 32)
					mem.ServicePort = uint32(port)
					fields++
				case "isLearner":
					if val == "true" {
						mem.IsLearner = true
					} else if val == "false" {
						mem.IsLearner = false
					}
				default:
				}
				if err != nil {
					break
				}
			}
			if err != nil || fields != 4 {
				c.App.PrintError(ErrAddMemberArgumentsInvalid)
				return nil
			}
			members, err := exeAddMemberFunc(mem)
			if err != nil {
				c.App.PrintError(err)
				return nil
			}
			_, _ = c.App.Println("success")
			_, _ = c.App.Println(members)
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
			members, err := exePromoteMemberFunc(nodeID)
			if err != nil {
				c.App.PrintError(err)
				return nil
			}
			_, _ = c.App.Println("success")
			_, _ = c.App.Println(members)
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
			members, err := exeRemoveMemberFunc(nodeID)
			if err != nil {
				c.App.PrintError(err)
				return nil
			}
			_, _ = c.App.Println("success")
			_, _ = c.App.Println(members)
			return nil
		},
	}
	cmdClusterStatus = &grb.Command{
		Name:    "CSTATUS",
		Aliases: []string{"cstatus"},
		Help:    "get cluster status",
		Usage:   "CSTATUS [linearizable] [ID]",
		Args: func(a *grb.Args) {
			a.Bool(argLinearizable, "whether to do linearizable reads", grb.Default(false))
			a.String(argID, "assigned node ID", grb.Default(randomSelectedID))
		},
		Run: func(c *grb.Context) error {
			linearizable := c.Args.Bool(argLinearizable)
			var ID *uint64
			sID := c.Args.String(argID)
			if sID != randomSelectedID {
				_ID, err := strconv.ParseUint(sID, 10, 64)
				if err != nil {
					c.App.PrintError(err)
					return nil
				}
				ID = &_ID
			}
			status, err := exeClusterStatusFUnc(linearizable, ID)
			if err != nil {
				c.App.PrintError(err)
				return nil
			}
			_, _ = c.App.Println("success")
			_, _ = c.App.Println(fmt.Sprintf("Cluster ID: %v, Cluster Name: %v", status.ID, status.Name))
			for _, mem := range status.Members {
				_, _ = c.App.Println(fmt.Sprintf("%+v", mem))
			}
			_, _ = c.App.Println(fmt.Sprintf("%+v", status.RemovedIDs))
			return nil
		},
	}

	// endregion

	// region raft cmd

	cmdRaftStatus = &grb.Command{
		Name:    "RSTATUS",
		Aliases: []string{"rstatus"},
		Help:    "get raft status",
		Usage:   "RSTATUS [linearizable] [ID]",
		Args: func(a *grb.Args) {
			a.Bool(argLinearizable, "whether to do linearizable reads", grb.Default(false))
			a.String(argID, "assigned node ID", grb.Default(randomSelectedID))
		},
		Run: func(c *grb.Context) error {
			linearizable := c.Args.Bool(argLinearizable)
			var ID *uint64
			sID := c.Args.String(argID)
			if sID != randomSelectedID {
				_ID, err := strconv.ParseUint(sID, 10, 64)
				if err != nil {
					c.App.PrintError(err)
					return nil
				}
				ID = &_ID
			}
			status, err := exeRaftStatusFunc(linearizable, ID)
			if err != nil {
				c.App.PrintError(err)
				return nil
			}
			_, _ = c.App.Println("success")
			_, _ = c.App.Println(fmt.Sprintf(
				"local ID: %v, local State: %v, local Term: %v, local Lead: %v, AppliedIndex: %v, CommittedIndex: %v",
				status.NodeID, status.RaftState, status.Term, status.Lead, status.AppliedIndex, status.CommittedIndex,
			))
			for _, pg := range status.Progresses {
				_, _ = c.App.Println(fmt.Sprintf("%+v", pg))
			}
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
			err := exeTransferLeaderFunc(fromID, toID)
			if err != nil {
				c.App.PrintError(err)
				return nil
			}
			_, _ = c.App.Println("success")
			return nil
		},
	}

	// endregion

	// region client cmd

	cmdHosts = &grb.Command{
		Name:    "HOSTS",
		Aliases: []string{"hosts"},
		Help:    "get all client addresses",
		Usage:   "HOSTS",
		Args: func(a *grb.Args) {
		},
		Run: func(c *grb.Context) error {
			hosts, err := exeHostsFunc()
			if err != nil {
				c.App.PrintError(err)
				return nil
			}
			_, _ = c.App.Println("success")
			_, _ = c.App.Println(strings.Join(hosts, ";"))
			return nil
		},
	}

	cmdResolve = &grb.Command{
		Name:    "RESOLVE",
		Aliases: []string{"resolve"},
		Help:    "resolve all client addresses via remote nodes",
		Usage:   "RESOLVE",
		Args: func(a *grb.Args) {
		},
		Run: func(c *grb.Context) error {
			err := exeResolveFunc()
			if err != nil {
				c.App.PrintError(err)
				return nil
			}
			_, _ = c.App.Println("success")
			return nil
		},
	}

	// endregion
)

func init() {
	var interceptCmdRun = func(inner func(c *grb.Context) error) func(c *grb.Context) error {
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

			case cmdHosts.Name:
				fallthrough
			case cmdResolve.Name:
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

	for _, cmd := range [...]*grb.Command{
		cmdGet, cmdPut, cmdDel, cmdRangeBegin, cmdRangeNext, cmdRangeClose,
		cmdAddMember, cmdPromoteMember, cmdRemoveMember, cmdClusterStatus,
		cmdRaftStatus, cmdTransferLeader,
		cmdHosts, cmdResolve,
	} {
		cmd.Run = interceptCmdRun(cmd.Run)
		app.AddCommand(cmd)
	}

	app.OnInit(func(a *grb.App, flags grb.FlagMap) error {
		host := flags.String(flagHost)
		urls := strings.Split(host, ";")
		return appInit(urls)
	})
	app.OnClose(func() error {
		return closer()
	})
}

func Run() error {
	return app.Run()
}
