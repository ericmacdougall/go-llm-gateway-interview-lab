package lockwire

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
)

type replyGate struct {
	client   net.Conn
	armed    <-chan struct{}
	withheld chan struct{}
	once     sync.Once
}

func (g *replyGate) Write(p []byte) (int, error) {
	select {
	case <-g.armed:
		if len(p) != 0 {
			g.once.Do(func() { close(g.withheld) })
		}
		return len(p), nil
	default:
		return g.client.Write(p)
	}
}

type faultProxy struct {
	address  *net.TCPAddr
	armed    chan struct{}
	withheld chan struct{}
}

// Only the first connection carries the gate. Reconnects and cancellation
// requests use ordinary forwarding. PostgreSQL is reached via its local socket.
func startFaultProxy(t *testing.T, socket string) *faultProxy {
	t.Helper()
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	proxy := &faultProxy{
		address:  listener.Addr().(*net.TCPAddr),
		armed:    make(chan struct{}),
		withheld: make(chan struct{}),
	}
	var relays sync.WaitGroup
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		first := true
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}

			var gate *replyGate
			if first {
				first = false
				gate = &replyGate{client: client, armed: proxy.armed, withheld: proxy.withheld}
			}
			relays.Add(1)
			go func(client net.Conn, gate *replyGate) {
				defer relays.Done()
				defer client.Close()
				postgres, err := (&net.Dialer{}).DialContext(ctx, "unix", socket)
				if err != nil {
					return
				}
				defer postgres.Close()
				stop := context.AfterFunc(ctx, func() {
					_ = client.Close()
					_ = postgres.Close()
				})
				defer stop()
				relay(client, postgres, gate)
			}(client, gate)
		}
	}()
	t.Cleanup(func() {
		cancel()
		_ = listener.Close()
		<-acceptDone
		relays.Wait()
	})
	return proxy
}

func relay(client, postgres net.Conn, gate *replyGate) {
	done := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(postgres, client)
		done <- struct{}{}
	}()
	go func() {
		var destination io.Writer = client
		if gate != nil {
			destination = gate
		}
		_, _ = io.Copy(destination, postgres)
		done <- struct{}{}
	}()

	// Withholding a reply doesn't end either copy. A real endpoint closure
	// ends the relay and closes the other side.
	<-done
	_ = client.Close()
	_ = postgres.Close()
	<-done
}
