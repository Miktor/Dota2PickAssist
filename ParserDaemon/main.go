// main
package main

import (
	"./parser"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

type DbConfig struct {
	Host     string `json:"host"`
	Port     string `json:"port"`
	Login    string `json:"login"`
	Password string `json:"password"`
	DbName   string `json:"db_name"`
}

type Config struct {
	SteamApiKey string   `json:"steam_api_key"`
	Db          DbConfig `json:"db"`
	LogFile     string   `json:"log_file"`
}

func LoadConfig(filePath string) (cfg Config) {
	file, e := ioutil.ReadFile(filePath)
	if e != nil {
		fmt.Printf("File error: %v\n", e)
		os.Exit(1)
	}

	fmt.Println("Loading config")
	json.Unmarshal(file, &cfg)
	return cfg
}

func main() {
	config := LoadConfig("config.json")

	f, err := os.OpenFile(config.LogFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("File error: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	log.SetOutput(f)
	log.Println("Starting")
	parser.Start(config.SteamApiKey)
	log.Println("Exiting")
}
