// Package red implements server speaking redis serialization protocol.
package red

import (
	"bufio"
	"io"
	"net"
	"strings"
	"time"

	"github.com/artyom/resp"
)

// HandlerFunc is a type of command processing function. It should return value
// that's passed to resp.Encode function, so it's expected such function would
// return values of resp package types.
type HandlerFunc func(req Request) (interface{}, error)

// Request holds information about single redis command.
type Request struct {
	Name string   // lowercase command itself (first word)
	Args []string // command arguments
}

// NewServer returns initialized server.
func NewServer() *Server {
	return &Server{log: noopLogger{}}
}

// WithLogger configures server to use provided Logger.
func (s *Server) WithLogger(l Logger) {
	if l != nil {
		s.log = l
	}
}

// Handle registers handler for command with given name
func (s *Server) Handle(name string, h HandlerFunc) {
	if s.handlers == nil {
		s.handlers = make(map[string]HandlerFunc)
	}
	s.handlers[name] = h
}

// Server implements server speaking RESP (REdis Serialization Protocol). Server
// automatically handles MULTI & EXEC commands for transactions, other commands
// are expected to be implemented separately and registered with Handle method.
type Server struct {
	log      Logger
	handlers map[string]HandlerFunc
}

// HandleConn processes single client connection, automatically handling
// transactions (MULTI/EXEC commands). It calls user-provided handlers for
// registered commands.
func (s *Server) HandleConn(conn io.ReadWriteCloser) error {
	defer conn.Close()
	rd := bufio.NewReader(conn)
	var tx []Request
	var inTx bool  // if we're inside transaction
	var errTx bool // true if transaction seen error and should be discarded
	var err error
	for {
		if err != nil {
			return err
		}
		req, err := resp.DecodeRequest(rd)
		switch err {
		case nil:
		case resp.ErrInvalidRequest:
			err = resp.Encode(conn, resp.Error("ERR unknown command"))
			continue
		default:
			return err
		}
		cmd := strings.ToLower(req[0])
		s.log.Println("REQ:", req)
		switch cmd {
		case "multi":
			if len(req) != 1 {
				if inTx {
					errTx = true
				}
				err = resp.Encode(conn, ErrWrongArgs(cmd))
				continue
			}
			if inTx {
				errTx = true
				err = resp.Encode(conn, resp.Error("ERR MULTI calls can not be nested"))
				continue
			}
			inTx, errTx = true, false
			err = resp.Encode(conn, resp.OK{})
			continue
		case "exec":
			if len(req) != 1 {
				if inTx {
					errTx = true
				}
				err = resp.Encode(conn, ErrWrongArgs(cmd))
				continue
			}
			if !inTx {
				err = resp.Encode(conn, resp.Error("ERR EXEC without MULTI"))
				continue
			}
			if errTx {
				inTx, errTx = false, false
				tx = tx[:0]
				err = resp.Encode(conn, resp.Error("EXECABORT Transaction discarded because of previous errors."))
				continue
			}
		default:
			h, ok := s.handlers[cmd]
			if !ok {
				if inTx {
					errTx = true
				}
				err = resp.Encode(conn, errNoCmd(cmd))
				continue
			}
			if inTx {
				if !errTx {
					tx = append(tx, Request{Name: cmd, Args: req[1:]})
				}
				err = resp.Encode(conn, resp.SimpleString("QUEUED"))
				continue
			}
			err = resp.Encode(conn, singleVal(h(Request{Name: cmd, Args: req[1:]})))
			continue
		}

		txReplies := make(resp.Array, 0, len(tx))
		for _, r := range tx {
			h, ok := s.handlers[r.Name]
			if !ok {
				txReplies = append(txReplies, errNoCmd(r.Name))
				continue
			}
			txReplies = append(txReplies, singleVal(h(r)))
		}
		inTx, errTx = false, false
		tx = tx[:0]
		err = resp.Encode(conn, txReplies)
	}
}

// ListenAndServe listens on TCP network address addr and then calls Serve to
// handle requests on incoming connections.
func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.Serve(tcpKeepAliveListener{ln.(*net.TCPListener)})
}

// Serve accepts incoming connections on the Listener l, creating a new service
// goroutine for each.
func (s *Server) Serve(l net.Listener) error {
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}
		go func(c net.Conn) {
			switch err := s.HandleConn(c); err {
			case nil, io.EOF:
			default:
				if s.log != nil {
					s.log.Println(err)
				}
			}
		}(conn)
	}
}

// Logger is a set of methods used to log information. *log.Logger implements
// this interface.
type Logger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

type noopLogger struct{}

func (noopLogger) Print(v ...interface{})                 {}
func (noopLogger) Printf(format string, v ...interface{}) {}
func (noopLogger) Println(v ...interface{})               {}

func errNoCmd(name string) resp.Error { return resp.Error("ERR unknown command '" + name + "'") }

// ErrWrongArgs returns resp.Error saying that command has wrong number of
// arguments
func ErrWrongArgs(name string) resp.Error {
	return resp.Error("ERR wrong number of arguments for '" + name + "' command")
}

// silgleVal returns v if err is nil, otherwise it returns resp.Error holding
// err text. Intended to be used as a wrapper for HandlerFunc
func singleVal(v interface{}, err error) interface{} {
	if err != nil {
		return resp.Error("ERR " + err.Error())
	}
	return v
}

// tcpKeepAliveListener sets TCP keep-alive timeouts on accepted
// connections. It's used by ListenAndServe and ListenAndServeTLS so
// dead TCP connections (e.g. closing laptop mid-download) eventually
// go away.
type tcpKeepAliveListener struct {
	*net.TCPListener
}

func (ln tcpKeepAliveListener) Accept() (c net.Conn, err error) {
	tc, err := ln.AcceptTCP()
	if err != nil {
		return
	}
	tc.SetKeepAlive(true)
	tc.SetKeepAlivePeriod(3 * time.Minute)
	return tc, nil
}
