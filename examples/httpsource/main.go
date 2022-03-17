package main

import (
	"log"

	"github.com/bitstrapped/airbyte"
	"github.com/bitstrapped/airbyte/examples/httpsource/apisource"
)

func main() {
	hsrc := apisource.NewAPISource("https://api.bitstrapped.com")
	runner := airbyte.NewSourceRunner(hsrc)
	err := runner.Start()
	if err != nil {
		log.Fatal(err)
	}
}
