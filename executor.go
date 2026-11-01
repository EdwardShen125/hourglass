package hourglass

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
)

// The executor face: remote delivery over gRPC (DESIGN "Remote
// delivery"). Handwritten message types implement proto.Message via
// protoadapt-free embedding — Go protobuf allows any struct with
// ProtoReflect; to stay dependency-light we define the service with
// plain Go types and a grpc.UnaryServer with manual marshal via
// protobuf wire using proto.Marshal on generated-equivalent structs is
// not possible without codegen — so we use a simple gRPC service built
// on grpc.ServiceDesc with raw codecs (JSON payloads inside gRPC
// frames). This keeps the wire format stable and auditable.

// ExecRequest is one execution command pushed to a remote executor.
type ExecRequest struct {
	FiredId string // receipt id: the ack identity (lesson #4)
	JobId   string
	Topic   string
	Payload []byte
	Attempt int
}

// ExecAck answers one request. RescheduleAt (unix seconds, 0 = none)
// requeues the job — self-chaining reserved from day one.
type ExecAck struct {
	Ok           bool
	Err          string
	RescheduleAt int64
}

// codecJSON is a minimal gRPC codec (name "json"): exec face payloads
// are small; JSON keeps the wire debuggable and avoids protobuf codegen
// in the library's build path.
type codecJSON struct{}

func (codecJSON) Marshal(v any) ([]byte, error) { return jsonMarshal(v) }
func (codecJSON) Unmarshal(data []byte, v any) error {
	return jsonUnmarshal(data, v)
}
func (codecJSON) Name() string { return "json" }

const execServiceName = "hourglass.Executor"

func init() {
	// register the JSON codec so both dialer and server accept
	// content-subtype "json"
	encoding.RegisterCodec(codecJSON{})
}

// executorServer adapts a function to the exec gRPC service.
type executorServer struct {
	fn func(ctx context.Context, req *ExecRequest) (*ExecAck, error)
}

// RegisterExecutorServer registers the executor service on a gRPC server.
func RegisterExecutorServer(srv *grpc.Server, es *executorServer) {
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: execServiceName,
		HandlerType: (*interface{})(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Exec",
			Handler: func(srvObj any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
				in := new(ExecRequest)
				if err := dec(in); err != nil {
					return nil, err
				}
				if interceptor != nil {
					info := &grpc.UnaryServerInfo{
						Server:     srvObj,
						FullMethod: "/" + execServiceName + "/Exec",
					}
					handler := func(ctx context.Context, req any) (any, error) {
						return es.fn(ctx, req.(*ExecRequest))
					}
					return interceptor(ctx, in, info, handler)
				}
				return es.fn(ctx, in)
			},
		}},
		Streams:  nil,
		Metadata: nil,
	}, es)
}

// ExecutorClient is the client side of the exec face.
type ExecutorClient struct {
	cc     *grpc.ClientConn
	callMu sync.Mutex
}

// NewExecutorClient builds an exec client on a live connection.
func NewExecutorClient(cc *grpc.ClientConn) *ExecutorClient {
	return &ExecutorClient{cc: cc}
}

// Exec pushes one command.
func (c *ExecutorClient) Exec(ctx context.Context, req *ExecRequest) (*ExecAck, error) {
	out := new(ExecAck)
	err := c.cc.Invoke(ctx, "/"+execServiceName+"/Exec", req, out,
		grpc.CallContentSubtype("json"))
	if err != nil {
		return nil, err
	}
	return out, nil
}

// SetExecutor points the engine's fired jobs at a remote executor: on
// fire, instead of (or after) a local handler miss, the job is pushed
// over gRPC; ack by fired-id completes it, reschedule_at requeues it, a
// failed ack consumes an attempt.
func (e *engine) SetExecutor(addr string, timeout time.Duration) {
	conn, err := grpc.Dial(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.CallContentSubtype("json")),
	)
	if err != nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.exec = &executorTarget{cl: NewExecutorClient(conn), conn: conn, timeout: timeout}
}

type executorTarget struct {
	cl      *ExecutorClient
	conn    *grpc.ClientConn
	timeout time.Duration
}

// deliver pushes one fired job to the remote executor. Returns
// (rescheduleAt, error): a nil error means acked (complete or
// reschedule); an error means the attempt failed.
func (e *engine) deliver(j Job, firedID string) (rescheduleAt int64, err error) {
	e.mu.Lock()
	ex := e.exec
	e.mu.Unlock()
	if ex == nil {
		return 0, fmt.Errorf("hourglass: no executor configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), ex.timeout)
	defer cancel()
	ack, err := ex.cl.Exec(ctx, &ExecRequest{
		FiredId: firedID, JobId: j.ID, Topic: j.Topic, Payload: j.Payload, Attempt: j.Attempts,
	})
	if err != nil {
		return 0, err
	}
	if !ack.Ok {
		return 0, fmt.Errorf("hourglass: executor rejected: %s", ack.Err)
	}
	return ack.RescheduleAt, nil
}

func (e *engine) closeExecutor() {
	e.mu.Lock()
	ex := e.exec
	e.exec = nil
	e.mu.Unlock()
	if ex != nil {
		ex.conn.Close()
	}
}

func jsonMarshal(v any) ([]byte, error) { return json.Marshal(v) }

func jsonUnmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
