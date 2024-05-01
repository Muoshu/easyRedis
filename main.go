package main

import (
	"easyRedis/config"
	"easyRedis/logger"
	"easyRedis/memdb"
	"easyRedis/server"
	"fmt"
	_ "net/http/pprof"
	"os"
)

func init() {
	// Register commands
	memdb.RegisterKeyCommands()
	memdb.RegisterStringCommands()
	memdb.RegisterListCommands()
	memdb.RegisterSetCommands()
	memdb.RegisterHashCommands()
	memdb.RegisterSortSetCommands()
}

func main() {

	cfg, err := config.Setup()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	err = logger.Setup(cfg)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	err = server.Start(cfg)

	if err != nil {
		os.Exit(1)
	}

}
