package hourglass

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Executor face: remote delivery over gRPC (DESIGN "Remote delivery").
// The fired_record row is the durable queue; the executor acks by
// fired-id; ack carries an optional reschedule_at (reserved from day
// one in DESIGN implementation notes).

func startExecutorServer(t *testing.T, fn func(ctx context.Context, req *ExecRequest) (*ExecAck, error)) (*grpc.Server, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	RegisterExecutorServer(srv, &executorServer{fn: fn})
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return srv, lis.Addr().String()
}

func TestExecutorDeliversAndAcks(t *testing.T) {
	_, addr := startExecutorServer(t, func(ctx context.Context, req *ExecRequest) (*ExecAck, error) {
		return &ExecAck{Ok: true}, nil
	})

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	cl := NewExecutorClient(conn)

	ack, err := cl.Exec(context.Background(), &ExecRequest{
		FiredId: "f1", JobId: "j1", Topic: "t", Payload: []byte(`{}`), Attempt: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ack.Ok {
		t.Fatal("executor acked failure")
	}
}

func TestEngineDeliversToRemoteExecutor(t *testing.T) {
	// engine with an executor target: fired jobs are pushed over gRPC,
	// acked by fired-id, completed on ack; a failed ack retries.
	e, clk := newTestEngine(t)
	ctx := context.Background()

	acks := make(chan string, 8)
	_, addr := startExecutorServer(t, func(ctx context.Context, req *ExecRequest) (*ExecAck, error) {
		acks <- req.JobId
		return &ExecAck{Ok: true}, nil
	})
	e.SetExecutor(addr, 50*time.Millisecond)

	id, err := e.Submit(ctx, "remote", nil, At(clk.Now().Add(2*time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	e.step(ctx)
	e.pump(clk.Now().Add(3 * time.Second))

	select {
	case got := <-acks:
		if got != id {
			t.Fatalf("executor saw %s want %s", got, id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("remote executor never received the job")
	}
	waitJobState(t, e, id, Completed)
}

func TestExecutorFailureRetriesThenDeadLetters(t *testing.T) {
	e, clk := newTestEngine(t)
	ctx := context.Background()

	var calls int
	_, addr := startExecutorServer(t, func(ctx context.Context, req *ExecRequest) (*ExecAck, error) {
		calls++
		return &ExecAck{Ok: false, Err: "executor down"}, nil
	})
	e.SetExecutor(addr, 30*time.Millisecond)

	id, _ := e.Submit(ctx, "remote", nil, At(clk.Now().Add(time.Second)), Retries(2), RetryBackoff(50*time.Millisecond))
	e.step(ctx)
	e.pump(clk.Now().Add(2 * time.Second)) // attempt 1 fails

	// backoff requeue: drive attempts 2 and 3 past their backoff dues
	for i := 0; i < 4; i++ {
		clk.Advance(100 * time.Millisecond)
		e.step(ctx)
		e.pump(clk.Now().Add(time.Second))
		time.Sleep(50 * time.Millisecond)
		j, _ := e.Get(ctx, id)
		if j.State == Dead {
			break
		}
	}

	// attempts exhaust -> dead
	waitJobState(t, e, id, Dead)
	if calls < 2 {
		t.Fatalf("executor called %d times, want >= 2 (retries)", calls)
	}
}

func TestAckRescheduleAt(t *testing.T) {
	// executor ack carrying reschedule_at requeues the job (self-
	// chaining, reserved field from day one).
	e, clk := newTestEngine(t)
	ctx := context.Background()

	_, addr := startExecutorServer(t, func(ctx context.Context, req *ExecRequest) (*ExecAck, error) {
		return &ExecAck{Ok: true, RescheduleAt: clk.Now().Add(2 * time.Second).Unix()}, nil
	})
	e.SetExecutor(addr, 30*time.Millisecond)

	id, _ := e.Submit(ctx, "chain", nil, At(clk.Now().Add(time.Second)), Retries(2))
	e.step(ctx)
	e.pump(clk.Now().Add(2 * time.Second))
	waitJobState(t, e, id, Waiting) // requeued by the ack

	j, _ := e.Get(ctx, id)
	if !j.Due.After(clk.Now()) {
		t.Fatalf("rescheduled due %v not in the future", j.Due)
	}
}
