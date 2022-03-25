package main

import (
	"log"
	"os"

	"github.com/bitstrapped/airbyte"
	"github.com/bitstrapped/airbyte/examples/httpsource/apisource"
)

func main() {
	hsrc := apisource.NewAPISource("https://api.bitstrapped.com")
	runner := airbyte.NewSourceRunner(hsrc, os.Stdout)
	err := runner.Start()
	if err != nil {
		log.Fatal(err)
	}
}
