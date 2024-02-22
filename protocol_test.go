package airbyte

import (
	"bytes"
	"testing"
)

func TestStreamState(t *testing.T) {
	buff := bytes.NewBuffer(nil)
	sw := newStateWriter(buff)

	sd := StreamDescriptor{
		Name:      "test",
		Namespace: "testnamespace",
	}

	streamData := struct {
		FieldA string
		FieldB int
	}{
		FieldA: "test",
		FieldB: 1,
	}

	err := sw(sd, streamData)
	if err != nil {
		t.Error(err)
	}

}
