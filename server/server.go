package server

import (
	"easyRedis/config"
	"easyRedis/logger"
	"log"
	"net"
	"strconv"
	"sync"
)

// Start starts a simple redis server
func Start(cfg *config.Config) error {

	listener, err := net.Listen("tcp", cfg.Host+":"+strconv.Itoa(cfg.Port))
	if err != nil {
		log.Panicln(err)
		return err
	}
	defer func(listener net.Listener) {
		err := listener.Close()
		if err != nil {

		}
	}(listener)

	logger.Info("server listen at ", cfg.Host, ":", cfg.Port)

	var wg sync.WaitGroup
	handler := NewHandler()

	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Error(err)
			break
		}
		logger.Info(conn.RemoteAddr().String(), " connected")
		wg.Add(1)
		go func() {
			defer wg.Done()
			handler.Handle(conn)
		}()
	}
	wg.Wait()
	return nil
}
