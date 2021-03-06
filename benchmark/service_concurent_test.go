package benchmark

import (
	"context"
	"fmt"
	"github.com/vigilglc/raft-lsm/client"
	"github.com/vigilglc/raft-lsm/server/api/rpcpb"
	"github.com/vigilglc/raft-lsm/server/backend/kvpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"sync/atomic"
	"testing"
	"time"
)

func Benchmark_Concurrent1_RaftLSM_Service_Put(b *testing.B) {
	benchmarkConcurrentRaftLSMServicePutCommon(b, 1, 1)
}
func Benchmark_Concurrent2_RaftLSM_Service_Put(b *testing.B) {
	benchmarkConcurrentRaftLSMServicePutCommon(b, 2, 1)
}
func Benchmark_Concurrent4_RaftLSM_Service_Put(b *testing.B) {
	benchmarkConcurrentRaftLSMServicePutCommon(b, 4, 1)

}
func Benchmark_Concurrent8_RaftLSM_Service_Put(b *testing.B) {
	benchmarkConcurrentRaftLSMServicePutCommon(b, 8, 1)
}
func benchmarkConcurrentRaftLSMServicePutCommon(b *testing.B, parallelism int, clientCount int) {
	fetchAgent, release, err := createRaftLSMClients(clientCount)
	if err != nil {
		b.Error(err)
	}
	defer release()
	b.SetParallelism(parallelism)
	b.ResetTimer()
	defer b.StopTimer()
	b.RunParallel(func(pb *testing.PB) {
		agt := fetchAgent()
		for pb.Next() {
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
	})
}

func Benchmark_Concurrent1_Linearizable_RaftLSM_Service_Get(b *testing.B) {
	benchmarkConcurrentLinearizableRaftLSMServiceGetCommon(b, 1)
}
func Benchmark_Concurrent2_Linearizable_RaftLSM_Service_Get(b *testing.B) {
	benchmarkConcurrentLinearizableRaftLSMServiceGetCommon(b, 2)
}
func Benchmark_Concurrent4_Linearizable_RaftLSM_Service_Get(b *testing.B) {
	benchmarkConcurrentLinearizableRaftLSMServiceGetCommon(b, 4)
}
func Benchmark_Concurrent8_Linearizable_RaftLSM_Service_Get(b *testing.B) {
	benchmarkConcurrentLinearizableRaftLSMServiceGetCommon(b, 8)
}
func benchmarkConcurrentLinearizableRaftLSMServiceGetCommon(b *testing.B, parallelism int) {
	fetchAgent, release, err := createRaftLSMClients(1)
	if err != nil {
		b.Error(err)
	}
	defer release()
	b.SetParallelism(parallelism)
	b.ResetTimer()
	defer b.StopTimer()
	b.RunParallel(func(pb *testing.PB) {
		agt := fetchAgent()
		for pb.Next() {
			_ = agt.Pick(func(ctx context.Context, c client.Client) error {
				_, err := c.Get(ctx, &rpcpb.GetRequest{
					Key:          generateValidKey(b),
					Linearizable: true,
				})
				return err
			})
		}
	})
}

func Benchmark_Concurrent1_Serializable_RaftLSM_Service_Get(b *testing.B) {
	benchmarkConcurrentSerializableRaftLSMServiceGetCommon(b, 1)
}
func Benchmark_Concurrent2_Serializable_RaftLSM_Service_Get(b *testing.B) {
	benchmarkConcurrentSerializableRaftLSMServiceGetCommon(b, 2)
}
func Benchmark_Concurrent4_Serializable_RaftLSM_Service_Get(b *testing.B) {
	benchmarkConcurrentSerializableRaftLSMServiceGetCommon(b, 4)
}
func Benchmark_Concurrent8_Serializable_RaftLSM_Service_Get(b *testing.B) {
	benchmarkConcurrentSerializableRaftLSMServiceGetCommon(b, 8)
}
func benchmarkConcurrentSerializableRaftLSMServiceGetCommon(b *testing.B, parallelism int) {
	fetchAgent, release, err := createRaftLSMClients(1)
	if err != nil {
		b.Error(err)
	}
	defer release()
	b.SetParallelism(parallelism)
	b.ResetTimer()
	defer b.StopTimer()
	b.RunParallel(func(pb *testing.PB) {
		agt := fetchAgent()
		for pb.Next() {
			_ = agt.Pick(func(ctx context.Context, c client.Client) error {
				_, err := c.Get(ctx, &rpcpb.GetRequest{
					Key:          generateValidKey(b),
					Linearizable: false,
				})
				return err
			})
		}
	})
}

func Benchmark_Concurrent1_Etcd_Service_Put(b *testing.B) {
	benchmarkConcurrentEtcdServicePutCommon(b, 1, 1)
}
func Benchmark_Concurrent2_Etcd_Service_Put(b *testing.B) {
	benchmarkConcurrentEtcdServicePutCommon(b, 2, 1)
}
func Benchmark_Concurrent4_Etcd_Service_Put(b *testing.B) {
	benchmarkConcurrentEtcdServicePutCommon(b, 4, 1)
}
func Benchmark_Concurrent8_Etcd_Service_Put(b *testing.B) {
	benchmarkConcurrentEtcdServicePutCommon(b, 8, 1)
}
func benchmarkConcurrentEtcdServicePutCommon(b *testing.B, parallelism int, clientCount int) {
	fetchClient, release, err := createEtcdClients(clientCount)
	if err != nil {
		b.Error(err)
	}
	defer release()
	ctx := context.Background()
	b.SetParallelism(parallelism)
	b.ResetTimer()
	defer b.StopTimer()
	b.RunParallel(func(pb *testing.PB) {
		cli := fetchClient()
		for pb.Next() {
			_, err := cli.Put(ctx, generateValidKey(b), string(newRandomBytes(256)))
			if err != nil {
				b.Logf("%v\n", err)
			}
		}
	})
}

func Benchmark_Concurrent1_Linearizable_Etcd_Service_Get(b *testing.B) {
	benchmarkConcurrentLinearizableEtcdServiceGetCommon(b, 1)
}
func Benchmark_Concurrent2_Linearizable_Etcd_Service_Get(b *testing.B) {
	benchmarkConcurrentLinearizableEtcdServiceGetCommon(b, 2)
}
func Benchmark_Concurrent4_Linearizable_Etcd_Service_Get(b *testing.B) {
	benchmarkConcurrentLinearizableEtcdServiceGetCommon(b, 4)
}
func Benchmark_Concurrent8_Linearizable_Etcd_Service_Get(b *testing.B) {
	benchmarkConcurrentLinearizableEtcdServiceGetCommon(b, 8)
}
func benchmarkConcurrentLinearizableEtcdServiceGetCommon(b *testing.B, parallelism int) {
	fetchClient, release, err := createEtcdClients(1)
	if err != nil {
		b.Error(err)
	}
	defer release()
	ctx := context.Background()
	b.SetParallelism(parallelism)
	b.ResetTimer()
	defer b.StopTimer()
	b.RunParallel(func(pb *testing.PB) {
		cli := fetchClient()
		for pb.Next() {
			_, _ = cli.Get(ctx, generateValidKey(b))
		}
	})
}

func Benchmark_Concurrent1_Serializable_Etcd_Service_Get(b *testing.B) {
	benchmarkConcurrentSerializableEtcdServiceGetCommon(b, 1)
}
func Benchmark_Concurrent2_Serializable_Etcd_Service_Get(b *testing.B) {
	benchmarkConcurrentSerializableEtcdServiceGetCommon(b, 2)
}
func Benchmark_Concurrent4_Serializable_Etcd_Service_Get(b *testing.B) {
	benchmarkConcurrentSerializableEtcdServiceGetCommon(b, 4)
}
func Benchmark_Concurrent8_Serializable_Etcd_Service_Get(b *testing.B) {
	benchmarkConcurrentSerializableEtcdServiceGetCommon(b, 8)
}
func benchmarkConcurrentSerializableEtcdServiceGetCommon(b *testing.B, parallelism int) {
	fetchClient, release, err := createEtcdClients(1)
	if err != nil {
		b.Error(err)
	}
	defer release()
	ctx := context.Background()
	b.SetParallelism(parallelism)
	b.ResetTimer()
	defer b.StopTimer()
	b.RunParallel(func(pb *testing.PB) {
		cli := fetchClient()
		for pb.Next() {
			_, _ = cli.Get(ctx, generateValidKey(b), clientv3.WithSerializable())
		}
	})
}

func createRaftLSMClients(count int) (fetchClient func() client.Agent, release func(), err error) {
	if count <= 0 {
		err = fmt.Errorf("no clients created")
		return
	}
	var agents []client.Agent
	for i := 0; i < count; i++ {
		agt := client.NewAgent(context.Background(), "localhost:400", "localhost:401", "localhost:402")
		agents = append(agents, agt)
	}
	var agtIdx uint64
	fetchClient = func() client.Agent {
		return agents[(atomic.AddUint64(&agtIdx, 1)-1)%uint64(count)]
	}
	release = func() {
		for i := 0; i < count; i++ {
			_ = agents[i].Close()
		}
	}
	return
}

func createEtcdClients(count int) (fetchClient func() *clientv3.Client, release func(), err error) {
	if count <= 0 {
		err = fmt.Errorf("no clients created")
		return
	}
	var clients []*clientv3.Client
	for i := 0; i < count; i++ {
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{"localhost:2370", "localhost:2371", "localhost:2372"},
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			for j := 0; j < i; j++ {
				_ = clients[j].Close()
			}
			return nil, nil, err
		}
		clients = append(clients, cli)
	}
	var agtIdx uint64
	fetchClient = func() *clientv3.Client {
		return clients[(atomic.AddUint64(&agtIdx, 1)-1)%uint64(count)]
	}
	release = func() {
		for i := 0; i < count; i++ {
			_ = clients[i].Close()
		}
	}
	return
}
