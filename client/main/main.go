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
	"strings"
)

func initApp(ctx context.Context,
	balancerGen func(ctx context.Context) client.Balancer,
	clientGen func(ctx context.Context, host string) client.Client) {
	balancer := balancerGen(ctx)
	ctx = balancer.Context()
	app.SetInitFunc(func(hosts []string) error {
		for _, host := range hosts {
			host = strings.TrimSpace(host)
			balancer.AddClient(clientGen(ctx, host))
		}
		return nil
	})

	app.SetExeGetFunc(func(key string, linearizable bool) (val string, err error) {
		cli, release := balancer.Pick()
		defer release()
		resp, err := cli.Get(ctx, &rpcpb.GetRequest{Key: key, Linearizable: linearizable})
		if err != nil {
			return "", err
		}
		return resp.Val, err
	})
	app.SetExePutFunc(func(kvs []*kvpb.KV) (err error) {
		cli, release := balancer.Pick()
		defer release()
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
	app.SetExeDelFunc(func(keys []string) (err error) {
		cli, release := balancer.Pick()
		defer release()
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
	app.SetExeRangeFunc(func(from, to string, asc, linearizable bool) (exe app.RangeExecutor, err error) {
		cli, release := balancer.Pick()
		rgCli, err := cli.Range(ctx)
		if err != nil {
			return nil, err
		}
		exe, err = NewRangeExecutor(rgCli, release)
		if err != nil {
			return
		}
		err = exe.Begin(from, to, asc, linearizable)
		return
	})

	app.SetExeAddMemberFunc(func(mem *rpcpb.Member) (members []*rpcpb.Member, err error) {
		cli, release := balancer.Pick()
		defer release()
		resp, err := cli.AddMember(ctx, &rpcpb.AddMemberRequest{Member: mem})
		if resp != nil {
			members = resp.Members
		}
		if err == nil {
			host := client.GetMemberHost(mem)
			balancer.AddClient(clientGen(ctx, host))
		}
		return
	})
	app.SetExePromoteMemberFunc(func(ID uint64) (members []*rpcpb.Member, err error) {
		cli, release := balancer.Pick()
		defer release()
		resp, err := cli.PromoteMember(ctx, &rpcpb.PromoteMemberRequest{NodeID: ID})
		if resp != nil {
			members = resp.Members
		}
		return
	})
	app.SetExeRemoveMemberFunc(func(ID uint64) (members []*rpcpb.Member, err error) {
		cli, release := balancer.Pick()
		defer release()
		resp, err := cli.RemoveMember(ctx, &rpcpb.RemoveMemberRequest{NodeID: ID})
		if resp != nil {
			members = resp.Members
		}
		if err == nil {
			balancer.RemoveByID(ID)
		}
		return
	})
	app.SetExeClusterStatusFunc(func(ID uint64, linearizable bool) (status *rpcpb.ClusterStatusResponse, err error) {
		cli, release := balancer.PickByID(ID)
		if cli == nil {
			release()
			cli, release = balancer.Pick()
		}
		defer release()
		status, err = cli.ClusterStatus(ctx, &rpcpb.ClusterStatusRequest{Linearizable: linearizable})
		return
	})

	app.SetExeRaftStatusFunc(func(ID uint64, linearizable bool) (status *rpcpb.StatusResponse, err error) {
		cli, release := balancer.PickByID(ID)
		if cli == nil {
			release()
			cli, release = balancer.Pick()
		}
		defer release()
		status, err = cli.Status(ctx, &rpcpb.StatusRequest{Linearizable: linearizable})
		return
	})
	app.SetExeTransferLeaderFunc(func(fromID, toID uint64) (err error) {
		cli, release := balancer.PickByID(fromID)
		if cli == nil {
			release()
			cli, release = balancer.Pick()
		}
		defer release()
		if cli == nil {
			return fmt.Errorf("failed to pick client connecting to node: %d", fromID)
		}
		_, err = cli.TransferLeader(ctx, &rpcpb.TransferLeaderRequest{TransfereeID: toID})
		return
	})
}

func main() {
	var clientGen = client.NewClient
	var testing bool
	for i, arg := range os.Args {
		if arg == "--test" {
			os.Args = append(os.Args[:i], os.Args[i+1:]...)
			testing = true
		}
	}
	if testing {
		clientGen = func(ctx context.Context, host string) (client client.Client) {
			return newFakeClient(host)
		}
		log.Println("testing mode...")
	}
	initApp(context.Background(), client.NewBalancer, clientGen)
	log.Fatal(app.Run())
}
