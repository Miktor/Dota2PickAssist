// main
package main

import (
	"./parser"
	"log"
	"os"
)

func LoadApiKey(file string) (key string) {
	f, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("Failed to open ApiKey")
		return
	}
	defer f.Close()

	data := make([]byte, 100)
	count, err := f.Read(data)
	if err != nil {
		log.Fatal(err)
	}

	key = string(data[:count])
	return key
}

func main() {
	f, err := os.OpenFile("testlogfile.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return
	}
	defer f.Close()

	log.SetOutput(f)

	log.Println("Starting...")

	key := LoadApiKey("ApiKey")
	parser.Start(key)

	log.Println("Exiting...")
}
