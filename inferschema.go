package airbyte

import (
	"encoding/json"
	"fmt"
	"github.com/bitstrapped/airbyte/schema"
	"reflect"
)

// Infer schema translates golang structs to JSONSchema format
func InferSchemaFromStruct(i interface{}, logTracker LogTracker) Properties {
	var prop Properties

	s, err := schema.Generate(reflect.TypeOf(i))
	if err != nil {
		logTracker.Log(LogLevelError, fmt.Sprintf("Generate schema error: %d", err))
		return prop
	}

	b, err := json.Marshal(s)
	if err != nil {
		logTracker.Log(LogLevelError, fmt.Sprintf("Json marshal schema error: %d", err))
		return prop
	}

	err = json.Unmarshal(b, &prop)
	if err != nil {
		logTracker.Log(LogLevelError, fmt.Sprintf("Unmarshal schema to PropSpec error: %d", err))
		return prop
	}

	return prop
}
