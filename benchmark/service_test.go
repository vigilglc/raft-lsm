package benchmark

import (
	"context"
	"github.com/vigilglc/raft-lsm/client"
	"github.com/vigilglc/raft-lsm/server/api/rpcpb"
	"github.com/vigilglc/raft-lsm/server/backend"
	"github.com/vigilglc/raft-lsm/server/backend/kvpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"testing"
	"time"
)

func Benchmark_Sequential_RaftLSM_Service_Put(b *testing.B) {
	agt := client.NewAgent(context.Background(), "localhost:400", "localhost:401", "localhost:402")
	defer func(agt client.Agent) { _ = agt.Close() }(agt)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := agt.Pick(func(ctx context.Context, c client.Client) error {
			_, err := c.Put(ctx, &rpcpb.PutRequest{KeyVal: &kvpb.KV{
				Key: generateValidKey(b),
				Val: string(newRandomBytes(256)),
			}})
			return err
		})
		if err != nil {
			b.Logf("%v\n", err)
		}
	}
}

func Benchmark_Sequential_Linearizable_RaftLSM_Service_Get(b *testing.B) {
	agt := client.NewAgent(context.Background(), "localhost:400", "localhost:401", "localhost:402")
	defer func(agt client.Agent) { _ = agt.Close() }(agt)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = agt.Pick(func(ctx context.Context, c client.Client) error {
			_, err := c.Get(ctx, &rpcpb.GetRequest{
				Key:          generateValidKey(b),
				Linearizable: true,
			})
			return err
		})
	}
}

func Benchmark_Sequential_Serializable_RaftLSM_Service_Get(b *testing.B) {
	agt := client.NewAgent(context.Background(), "localhost:400", "localhost:401", "localhost:402")
	defer func(agt client.Agent) { _ = agt.Close() }(agt)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = agt.Pick(func(ctx context.Context, c client.Client) error {
			_, err := c.Get(ctx, &rpcpb.GetRequest{
				Key:          generateValidKey(b),
				Linearizable: false,
			})
			return err
		})
	}
}

func Benchmark_Sequential_Etcd_Service_Put(b *testing.B) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2370", "localhost:2371", "localhost:2372"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		b.Error(err)
	}
	ctx := context.Background()
	defer func(cli *clientv3.Client) { _ = cli.Close() }(cli)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := cli.Put(ctx, generateValidKey(b), string(newRandomBytes(256)))
		if err != nil {
			b.Logf("%v\n", err)
		}
	}
}

func Benchmark_Sequential_Linearizable_Etcd_Service_Get(b *testing.B) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2370", "localhost:2371", "localhost:2372"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		b.Error(err)
	}
	ctx := context.Background()
	defer func(cli *clientv3.Client) { _ = cli.Close() }(cli)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cli.Get(ctx, generateValidKey(b))
	}
}

func Benchmark_Sequential_Serializable_Etcd_Service_Get(b *testing.B) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2370", "localhost:2371", "localhost:2372"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		b.Error(err)
	}
	ctx := context.Background()
	defer func(cli *clientv3.Client) { _ = cli.Close() }(cli)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cli.Get(ctx, generateValidKey(b), clientv3.WithSerializable())
	}
}

func generateValidKey(b *testing.B) string {
	var key string
	b.StopTimer()
	defer b.StartTimer()
GEN:
	key = string(newRandomBytes(8))
	if !backend.ValidateKey(key) {
		goto GEN
	}
	return key
}
