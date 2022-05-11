package main

import (
	"context"
	"fmt"
	"github.com/vigilglc/raft-lsm/client"
	"github.com/vigilglc/raft-lsm/client/app"
	"github.com/vigilglc/raft-lsm/server/api/rpcpb"
	"github.com/vigilglc/raft-lsm/server/backend/kvpb"
	"log"
	"os"
	"time"
)

func initApp(ctx context.Context, agentGen func(ctx context.Context, initHosts ...string) client.Agent) {
	var appToStop = make(chan struct{})
	var appStopped = make(chan struct{})

	agent := agentGen(ctx)

	var caller = func(ID *uint64, act func(ctx context.Context, client client.Client) error) error {
		if ID != nil {
			return agent.PickByID(*ID, act)
		}
		return agent.Pick(act)
	}

	app.SetExeGetFunc(func(key string, linearizable bool, ID *uint64) (val string, err error) {
		err = caller(ID, func(ctx context.Context, cli client.Client) error {
			var resp *rpcpb.GetResponse
			resp, err = cli.Get(ctx, &rpcpb.GetRequest{Key: key, Linearizable: linearizable})
			if err != nil {
				val = ""
			} else {
				val = resp.Val
			}
			return err
		})
		return
	})
	app.SetExePutFunc(func(kvs []*kvpb.KV) (err error) {
		err = caller(nil, func(ctx context.Context, cli client.Client) error {
			if len(kvs) == 1 {
				_, err = cli.Put(ctx, &rpcpb.PutRequest{KeyVal: kvs[0]})
			} else if len(kvs) > 1 {
				req := new(rpcpb.WriteRequest)
				for _, kv := range kvs {
					req.Batch = append(req.Batch, &rpcpb.BatchEntry{
						OP:     rpcpb.BatchEntry_PUT,
						KeyVal: kv,
					})
				}
				_, err = cli.Write(ctx, req)
			}
			return err
		})
		return
	})
	app.SetExeDelFunc(func(keys []string) (err error) {
		err = caller(nil, func(ctx context.Context, cli client.Client) error {
			if len(keys) == 1 {
				_, err = cli.Del(ctx, &rpcpb.DelRequest{Key: keys[0]})
			} else if len(keys) > 1 {
				req := new(rpcpb.WriteRequest)
				for _, k := range keys {
					req.Batch = append(req.Batch, &rpcpb.BatchEntry{
						OP:     rpcpb.BatchEntry_DEL,
						KeyVal: &kvpb.KV{Key: k},
					})
				}
				_, err = cli.Write(ctx, req)
			}
			return err
		})
		return
	})
	app.SetExeRangeFunc(func(from, to string, asc, linearizable bool, ID *uint64) (exe app.RangeExecutor, err error) {
		err = caller(ID, func(ctx context.Context, cli client.Client) error {
			var rgCli rpcpb.KVService_RangeClient
			rgCli, err = cli.Range(ctx)
			if err != nil {
				exe = nil
			}
			cli.Lock()
			exe, err = NewRangeExecutor(rgCli, func() { cli.Unlock() })
			if err != nil {
				_ = exe.Close()
				return err
			}
			return exe.Begin(from, to, asc, linearizable)
		})
		return

	})

	app.SetExeAddMemberFunc(func(mem *rpcpb.Member) (members []*rpcpb.Member, err error) {
		err = caller(nil, func(ctx context.Context, cli client.Client) error {
			var resp *rpcpb.AddMemberResponse
			resp, err = cli.AddMember(ctx, &rpcpb.AddMemberRequest{Member: mem})
			if resp != nil {
				members = resp.Members
			}
			if err == nil {
				host := client.GetMemberHost(mem)
				agent.AddClients([]string{host})
			}
			return err
		})
		return
	})
	app.SetExePromoteMemberFunc(func(ID uint64) (members []*rpcpb.Member, err error) {
		err = caller(nil, func(ctx context.Context, cli client.Client) error {
			var resp *rpcpb.PromoteMemberResponse
			resp, err := cli.PromoteMember(ctx, &rpcpb.PromoteMemberRequest{NodeID: ID})
			if resp != nil {
				members = resp.Members
			}
			return err
		})
		return
	})
	app.SetExeRemoveMemberFunc(func(ID uint64) (members []*rpcpb.Member, err error) {
		err = caller(nil, func(ctx context.Context, cli client.Client) error {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			var resp *rpcpb.RemoveMemberResponse
			resp, err = cli.RemoveMember(ctx, &rpcpb.RemoveMemberRequest{NodeID: ID})
			defer cancel()
			if resp != nil {
				members = resp.Members
			}
			return err
		})
		return
	})
	app.SetExeClusterStatusFunc(func(linearizable bool, ID *uint64) (status *rpcpb.ClusterStatusResponse, err error) {
		err = caller(ID, func(ctx context.Context, cli client.Client) error {
			status, err = cli.ClusterStatus(ctx, &rpcpb.ClusterStatusRequest{Linearizable: linearizable})
			return err
		})
		return
	})

	app.SetExeRaftStatusFunc(func(linearizable bool, ID *uint64) (status *rpcpb.StatusResponse, err error) {
		err = caller(ID, func(ctx context.Context, cli client.Client) error {
			status, err = cli.Status(ctx, &rpcpb.StatusRequest{Linearizable: linearizable})
			return err
		})
		return
	})
	app.SetExeTransferLeaderFunc(func(fromID, toID uint64) (err error) {
		err = caller(&fromID, func(ctx context.Context, cli client.Client) error {
			_, err = cli.TransferLeader(ctx, &rpcpb.TransferLeaderRequest{TransfereeID: toID})
			return err
		})
		return
	})

	app.SetExeIDsFunc(func() (IDs []uint64, err error) {
		return agent.AllIDs(), nil
	})

	app.SetExeHostsFunc(func() (hosts []string, err error) {
		return agent.AllHosts(), nil
	})

	app.SetExeResolveFunc(agent.Resolve)

	app.SetInitFunc(func(hosts []string) error {
		agent.AddClients(hosts)
		if len(agent.AllHosts()) == 0 {
			return fmt.Errorf("failed to start client, since no server hosts provided")
		}
		_ = agent.Resolve()
		return nil
	})
	app.SetCloseFunc(func() error {
		close(appToStop)
		<-appStopped
		return agent.Close()
	})
}

func main() {
	var agentGen = client.NewAgent
	var testing bool
	for i, arg := range os.Args {
		if arg == "--test" {
			os.Args = append(os.Args[:i], os.Args[i+1:]...)
			testing = true
		}
	}
	if testing {
		// clientGen = func(ctx context.Context, host string) (client client.Client) {
		// 	return newFakeClient(host)
		// }
		// log.Println("testing mode...")
	}
	initApp(context.Background(), agentGen)
	log.Fatal(app.Run())
}
