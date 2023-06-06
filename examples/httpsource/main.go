package main

import (
	"log"
	"os"

	airbyte "github.com/kollalabs/airbyte-go"
	"github.com/kollalabs/airbyte-go/examples/httpsource/apisource"
)

func main() {
	hsrc := apisource.NewAPISource("https://api.bitstrapped.com")
	runner := airbyte.NewSourceRunner(hsrc, os.Stdout)
	err := runner.Start()
	if err != nil {
		log.Fatal(err)
	}
}
