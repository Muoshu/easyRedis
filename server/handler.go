package server

import (
	"easyRedis/logger"
	"easyRedis/memdb"
	"easyRedis/resp"
	"io"
	"net"
)

// Handler handles all client requests to the server
// It holds a MemDb instance to exchange data with clients

type Handler struct {
	memDb *memdb.MemDb
}

func NewHandler() *Handler {
	return &Handler{
		memDb: memdb.NewMemDb(),
	}
}

func (h *Handler) Handle(conn net.Conn) {
	defer func() {
		err := conn.Close()
		if err != nil {
			logger.Error(err)
		}
		// 关闭timeWheel
		h.memDb.Stop()
	}()

	ch := resp.ParseStream(conn)
	for parseRes := range ch {
		if parseRes.Err != nil {
			if parseRes.Err == io.EOF {
				logger.Info("Close connection ", conn.RemoteAddr().String())
			} else {
				logger.Panic("Handle connection ", conn.RemoteAddr().String(), "panic: ", parseRes.Err.Error())
			}
			return
		}
		if parseRes.Data == nil {
			logger.Error("empty parsedRes.Data from ", conn.RemoteAddr().String())
			continue
		}
		arrayData, ok := parseRes.Data.(*resp.ArrayData)
		if !ok {
			logger.Error("parsedRes.Data is not ArrayData from ", conn.RemoteAddr().String())
			continue
		}
		cmd := arrayData.ToCommand()
		res := h.memDb.ExecCommand(cmd)

		if res != nil {
			_, err := conn.Write(res.ToBytes())
			if err != nil {
				logger.Error("Write response to ", conn.RemoteAddr().String(), " error: ", err.Error())
			}
		} else {
			errData := resp.NewErrorData("unknown error")
			_, err := conn.Write(errData.ToBytes())
			if err != nil {
				logger.Error("write response to ", conn.RemoteAddr().String(), " error: ", err.Error())
			}
		}
	}
}
