package main

import (
	"context"
	"strings"

	"github.com/pingcap/log"
	"github.com/tidwall/redcon"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/rawkv"
	"go.uber.org/zap"
)

func main() {
	addr := ":6380"
	pdAddr := "127.0.0.1:2379"

	log.L().Info("Welcome to Redis Proxy on TiKV", zap.String("address", addr), zap.String("PD address", pdAddr))

	client, err := rawkv.NewClient(context.TODO(), []string{pdAddr}, config.DefaultConfig().Security)
	if err != nil {
		log.L().Fatal("can not connect to TiKV cluster", zap.String("error", err.Error()))
	}

	s := newServer(addr, client)

	err = redcon.ListenAndServe(s.addr, s.handler, s.accept, s.closed)
	if err != nil {
		log.L().Fatal("critical error", zap.String("error", err.Error()))
	}
}

type server struct {
	addr string
	ps   redcon.PubSub

	kvclient *rawkv.Client
}

func newServer(addr string, client *rawkv.Client) *server {
	return &server{
		addr:     addr,
		kvclient: client,
	}
}

func (s *server) handler(conn redcon.Conn, cmd redcon.Command) {
	switch strings.ToLower(string(cmd.Args[0])) {
	default:
		conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
	case "ping":
		conn.WriteString("PONG")
	case "quit":
		conn.WriteString("OK")
		conn.Close()
	case "set":
		if len(cmd.Args) != 3 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}

		err := s.kvclient.Put(context.TODO(), cmd.Args[1], cmd.Args[2])
		if err != nil {
			conn.WriteError("ERR " + err.Error())
		} else {
			conn.WriteString("OK")
		}
	case "get":
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}

		val, err := s.kvclient.Get(context.TODO(), cmd.Args[1])
		if err != nil {
			conn.WriteError("ERR " + err.Error())
		} else if len(val) == 0 {
			conn.WriteNull()
		} else {
			conn.WriteBulk(val)
		}
	case "del":
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}

		err := s.kvclient.Delete(context.TODO(), cmd.Args[1])
		if err != nil {
			conn.WriteError("ERR " + err.Error())
		} else {
			// TODO return 0 when the value is not existed.
			conn.WriteInt(1)
		}
	case "publish":
		if len(cmd.Args) != 3 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		conn.WriteInt(s.ps.Publish(string(cmd.Args[1]), string(cmd.Args[2])))
	case "subscribe", "psubscribe":
		if len(cmd.Args) < 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		command := strings.ToLower(string(cmd.Args[0]))
		for i := 1; i < len(cmd.Args); i++ {
			if command == "psubscribe" {
				s.ps.Psubscribe(conn, string(cmd.Args[i]))
			} else {
				s.ps.Subscribe(conn, string(cmd.Args[i]))
			}
		}
	}
}

func (s *server) accept(conn redcon.Conn) bool {
	// Use this function to accept or deny the connection.
	log.L().Info("accept", zap.String("remote address", conn.RemoteAddr()))
	return true
}

func (s *server) closed(conn redcon.Conn, err error) {
	// This is called when the connection has been closed
	// log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
	if err != nil {
		log.L().Error("error when close", zap.String("error", err.Error()))
	}
}
