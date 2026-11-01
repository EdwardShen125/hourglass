package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"github.com/EdwardShen125/hourglass"
)

// hourglass-bench: load tests against real infra, one scenario at a
// time. Results print as a table; the report aggregates them.

func main() {
	var (
		scenario = flag.String("s", "", "scenario: cluster-throughput | precision | failover | exec")
		mysqlDSN = flag.String("mysql", os.Getenv("TEST_MYSQL_DSN"), "mysql dsn")
		etcdAddr = flag.String("etcd", os.Getenv("TEST_ETCD_ADDR"), "etcd endpoint")
		nJobs    = flag.Int("n", 20000, "job count")
		shards   = flag.Int("shards", 16, "shard count")
		workers  = flag.Int("workers", 8, "handlers per engine")
	)
	flag.Parse()
	if *scenario == "" {
		fmt.Println("scenarios: cluster-throughput, failover")
		os.Exit(2)
	}
	ctx := context.Background()
	switch *scenario {
	case "cluster-throughput":
		clusterThroughput(ctx, *mysqlDSN, *etcdAddr, *nJobs, *shards, *workers)
	case "failover":
		failover(ctx, *mysqlDSN, *etcdAddr, *shards)
	}
}

type result struct {
	Name   string
	Fields [][2]string
}

var results []result

func emit(r result) {
	results = append(results, r)
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintf(w, "== %s ==\n", r.Name)
	for _, f := range r.Fields {
		fmt.Fprintf(w, "%s:\t%s\n", f[0], f[1])
	}
	w.Flush()
}

func pct(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]float64{}, xs...)
	sort.Float64s(s)
	i := int(float64(len(s)) * p)
	if i >= len(s) {
		i = len(s) - 1
	}
	return s[i]
}

func fmtMs(ms float64) string {
	if ms < 1000 {
		return fmt.Sprintf("%.0fms", ms)
	}
	return fmt.Sprintf("%.1fs", ms/1000)
}

// scenario 1: sustained submit + fire through a real cluster.
func clusterThroughput(ctx context.Context, dsn, etcd string, n, shardN, workerN int) {
	node, err := hourglass.Join(hourglass.Config{
		MySQL: dsn, Etcd: []string{etcd}, LeaseTTL: 15 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	defer node.Close()
	shardList := make([]int, shardN)
	for i := range shardList {
		shardList[i] = i
	}
	for _, sh := range shardList {
		if err := node.AddShard(ctx, sh); err != nil {
			panic(err)
		}
	}

	var delivered atomic.Int64
	node.Register("bench", func(context.Context, hourglass.Job) error {
		delivered.Add(1)
		return nil
	})

	// phase 1: submit throughput (batched, parallel)
	start := time.Now()
	var wg sync.WaitGroup
	per := n / 8
	errCh := make(chan error, 8)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				if _, err := node.Submit(ctx, "bench", nil,
					hourglass.At(time.Now().UTC().Add(time.Duration(i%5000)*time.Millisecond+2*time.Second)),
					hourglass.InShard((w*per+i)%shardN)); err != nil {
					errCh <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	submitDur := time.Since(start)
	close(errCh)
	for err := range errCh {
		panic(err)
	}

	// phase 2: run and drain
	runCtx, stop := context.WithCancel(ctx)
	go node.Run(runCtx)
	fireStart := time.Now()
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) && delivered.Load() < int64(n) {
		time.Sleep(250 * time.Millisecond)
	}
	drainDur := time.Since(fireStart)
	stop()

	emit(result{Name: fmt.Sprintf("cluster-throughput n=%d shards=%d", n, shardN), Fields: [][2]string{
		{"submit", fmt.Sprintf("%d jobs in %s = %.0f/s", n, submitDur, float64(n)/submitDur.Seconds())},
		{"drain", fmt.Sprintf("%d fired in %s = %.0f fires/s", delivered.Load(), drainDur, float64(delivered.Load())/drainDur.Seconds())},
		{"delivered", fmt.Sprintf("%d/%d (loss=%.2f%%)", delivered.Load(), n, 100*float64(n-int(delivered.Load()))/float64(n))},
	}})
}

// scenario 3: kill the owner mid-storm; measure recovery.
func failover(ctx context.Context, dsn, etcd string, shardN int) {
	if shardN < 2 {
		shardN = 2
	}
	nodeA, err := hourglass.Join(hourglass.Config{MySQL: dsn, Etcd: []string{etcd}, Shards: []int{0}, LeaseTTL: 3 * time.Second})
	if err != nil {
		panic(err)
	}
	var deliveredA atomic.Int64
	nodeA.Register("bench", func(context.Context, hourglass.Job) error {
		deliveredA.Add(1)
		return nil
	})
	runA, stopA := context.WithCancel(ctx)
	go nodeA.Run(runA)

	// submit a burst into shard 0, due AFTER the kill so the in-flight
	// rows must survive the takeover
	const burst = 2000
	killIn := 4 * time.Second
	for i := 0; i < burst; i++ {
		nodeA.Submit(ctx, "bench", nil,
			hourglass.At(time.Now().UTC().Add(killIn+2*time.Second+time.Duration(i%200)*10*time.Millisecond)),
			hourglass.InShard(0))
	}
	time.Sleep(1 * time.Second) // A admits the whole burst into its wheel

	// KILL node A (hard): no handover
	nodeA.KillForTest()

	// node B picks the shard up after lease expiry
	nodeB, err := hourglass.Join(hourglass.Config{MySQL: dsn, Etcd: []string{etcd}, Shards: []int{}, LeaseTTL: 3 * time.Second})
	if err != nil {
		panic(err)
	}
	defer nodeB.Close()
	var deliveredB atomic.Int64
	nodeB.Register("bench", func(context.Context, hourglass.Job) error {
		deliveredB.Add(1)
		return nil
	})
	runB, stopB := context.WithCancel(ctx)
	defer stopB()
	go nodeB.Run(runB)

	failT0 := time.Now()
	// B retries AddShard until the lease frees
	for time.Since(failT0) < 30*time.Second {
		if err := nodeB.AddShard(ctx, 0); err == nil && len(nodeB.EnginesForTest()) > 0 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	takeover := time.Since(failT0)

	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) && deliveredA.Load()+deliveredB.Load() < burst {
		time.Sleep(250 * time.Millisecond)
	}
	stopA()
	emit(result{Name: "failover", Fields: [][2]string{
		{"takeover", fmt.Sprintf("lease-expiry takeover in %s", takeover.Round(time.Millisecond))},
		{"deliveredA(before kill)", fmt.Sprintf("%d", deliveredA.Load())},
		{"deliveredB(after takeover)", fmt.Sprintf("%d", deliveredB.Load())},
		{"total", fmt.Sprintf("%d/%d (loss=%.2f%%)", deliveredA.Load()+deliveredB.Load(), burst, 100*float64(burst-int(deliveredA.Load()+deliveredB.Load()))/float64(burst))},
	}})
}
