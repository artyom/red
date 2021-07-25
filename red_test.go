package red

import (
	"fmt"
	"io"
	"net"
	"testing"
)

func TestServerStats(t *testing.T) {
	srv := NewServer()
	srv.Handle("ping", func(req Request) (interface{}, error) {
		if len(req.Args) > 0 {
			return req.Args[0], nil
		}
		return "PONG", nil
	})
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() { srv.Serve(ln) }()
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	const pingCnt = 2
	for i := 0; i < pingCnt; i++ {
		fmt.Fprintf(conn, "*2\r\n$4\r\nPING\r\n")
		fmt.Fprintf(conn, "$12\r\nHello, world\r\n")
	}
	fmt.Fprintf(conn, "*1\r\n$4\r\nQUIT\r\n")
	if _, err := io.Copy(io.Discard, conn); err != nil {
		t.Fatal(err)
	}
	st := srv.Stats()
	if l := len(st); l != 1 {
		t.Fatalf("Stat call got %d elements, want 1: %+v", l, st)
	}
	want := CmdCount{Name: "ping", Cnt: pingCnt}
	if st[0] != want {
		t.Fatalf("got %+v, want %+v", st[0], want)
	}
}
