// main
package main

import (
	"./dal"
	"./parser"
	"encoding/json"
	"fmt"
	log "github.com/cihub/seelog"
	"io/ioutil"
	"os"
)

type Config struct {
	SteamApiKey string       `json:"steam_api_key"`
	Db          dal.DbConfig `json:"db"`
	LogFile     string       `json:"log_file"`
}

func LoadConfig(filePath string) (cfg Config) {
	file, e := ioutil.ReadFile(filePath)
	if e != nil {
		fmt.Printf("File error: %v\n", e)
		os.Exit(1)
	}

	fmt.Println("Loading config")
	json.Unmarshal(file, &cfg)

	fmt.Println("Api key = " + cfg.SteamApiKey)

	return cfg
}

func main() {
	defer log.Flush()
	logger, err := log.LoggerFromConfigAsFile("logconfig")
	if err != nil {
		fmt.Printf("log error: %v\n", err)
		os.Exit(1)
	}
	log.ReplaceLogger(logger)

	config := LoadConfig("config.json")

	log.Trace("Initializationg...\n")

	dal.Connect(config.Db)
	defer dal.Close()

	log.Trace("Starting...\n")
	parser.Start(config.SteamApiKey)
	log.Trace("Exit!\n")
}
