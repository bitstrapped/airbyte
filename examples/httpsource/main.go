package main

import (
	"bitstrapped/airbyte"
	"bitstrapped/airbyte/examples/httpsource/apisource"
	"log"
)

func main() {
	hsrc := apisource.NewAPISource("https://api.bitstrapped.com")
	runner := airbyte.NewSourceRunner(hsrc)
	err := runner.Start()
	if err != nil {
		log.Fatal(err)
	}
}
