package apisource

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/bitstrapped/airbyte"
)

type APISource struct {
	baseURL string
}

type LastSyncTime struct {
	Timestamp int64 `json:"timestamp"`
}

type HTTPConfig struct {
	APIKey string `json:"apiKey"`
}

func NewAPISource(baseURL string) airbyte.Source {
	return APISource{
		baseURL: baseURL,
	}
}

func (h APISource) Spec(logTracker airbyte.LogTracker) (*airbyte.ConnectorSpecification, error) {
	logTracker.Log(airbyte.LogLevelInfo, "Running Spec")
	return &airbyte.ConnectorSpecification{
		DocumentationURL:      "https://bitstrapped.com",
		ChangeLogURL:          "https://bitstrapped.com",
		SupportsIncremental:   false,
		SupportsNormalization: true,
		SupportsDBT:           true,
		SupportedDestinationSyncModes: []airbyte.DestinationSyncMode{
			airbyte.DestinationSyncModeOverwrite,
		},
		ConnectionSpecification: airbyte.ConnectionSpecification{
			Title:       "Example HTTP Source",
			Description: "This is an example http source for the docs's",
			Type:        "object",
			Required:    []airbyte.PropertyName{"apiKey"},
			Properties: airbyte.Properties{
				Properties: map[airbyte.PropertyName]airbyte.PropertySpec{
					"apiKey": {
						Description: "api key to access http source, valid uuid",
						Examples:    []string{"xxxx-xxxx-xxxx-xxxx"},
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{
								airbyte.String,
							},
						},
					},
				},
			},
		},
	}, nil
}

func (h APISource) Check(srcCfgPath string, logTracker airbyte.LogTracker) error {
	logTracker.Log(airbyte.LogLevelDebug, "validating api connection")
	var srcCfg HTTPConfig
	err := airbyte.UnmarshalFromPath(srcCfgPath, &srcCfg)
	if err != nil {
		return err
	}

	resp, err := http.Get(fmt.Sprintf("%s/ping?key=%s", h.baseURL, srcCfg.APIKey))
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return errors.New("invalid status")
	}

	return nil
}

func (h APISource) Discover(srcCfgPath string, logTracker airbyte.LogTracker) (*airbyte.Catalog, error) {
	var srcCfg HTTPConfig
	err := airbyte.UnmarshalFromPath(srcCfgPath, &srcCfg)
	if err != nil {
		return nil, err
	}

	return &airbyte.Catalog{Streams: []airbyte.Stream{{
		Name: "users",
		JSONSchema: airbyte.Properties{
			Properties: map[airbyte.PropertyName]airbyte.PropertySpec{
				"userid": {
					PropertyType: airbyte.PropertyType{
						Type:        []airbyte.PropType{airbyte.Integer, airbyte.Null},
						AirbyteType: airbyte.BigInteger},
					Description: "user ID - see the big int",
				},
				"name": {
					PropertyType: airbyte.PropertyType{
						Type: []airbyte.PropType{airbyte.String, airbyte.Null},
					},
					Description: "user name",
				},
			},
		},
		SupportedSyncModes: []airbyte.SyncMode{
			airbyte.SyncModeFullRefresh,
		},
		SourceDefinedCursor: false,
		Namespace:           "bitstrapped",
	},
		{
			Name: "payments",
			JSONSchema: airbyte.Properties{
				Properties: map[airbyte.PropertyName]airbyte.PropertySpec{
					"userid": {
						PropertyType: airbyte.PropertyType{
							Type:        []airbyte.PropType{airbyte.Integer, airbyte.Null},
							AirbyteType: airbyte.BigInteger},
						Description: "user ID - see the big int",
					},
					"paymentAmount": {
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.Integer, airbyte.Null},
						},
						Description: "payment amount",
					},
				},
			},
			SupportedSyncModes: []airbyte.SyncMode{
				airbyte.SyncModeFullRefresh,
			},
			SourceDefinedCursor: false,
			Namespace:           "bitstrapped",
		},
	}}, nil
}

type User struct {
	UserID int64  `json:"userid"`
	Name   string `json:"name"`
}

type Payment struct {
	UserID        int64 `json:"userid"`
	PaymentAmount int64 `json:"paymentAmount"`
}

func (h APISource) Read(sourceCfgPath string, prevStatePath string, configuredCat *airbyte.ConfiguredCatalog,
	tracker airbyte.MessageTracker) error {
	tracker.Log(airbyte.LogLevelInfo, "Running read")
	var src HTTPConfig
	err := airbyte.UnmarshalFromPath(sourceCfgPath, &src)
	if err != nil {
		return err
	}

	// see if there is a last sync
	var st LastSyncTime
	airbyte.UnmarshalFromPath(sourceCfgPath, &st)
	if st.Timestamp <= 0 {
		st.Timestamp = -1
	}

	for _, stream := range configuredCat.Streams {
		if stream.Stream.Name == "users" {
			var u []User
			uri := fmt.Sprintf("https://api.bistrapped.com/users?apiKey=%s", src.APIKey)
			resp, err := http.Get(uri)
			if err != nil {
				return err
			}
			err = json.NewDecoder(resp.Body).Decode(&u)
			if err != nil {
				return err
			}

			for _, ur := range u {
				err := tracker.Record(ur, stream.Stream.Name, stream.Stream.Namespace)
				if err != nil {
					return err
				}
			}
		}

		if stream.Stream.Name == "payments" {
			var p []Payment
			uri := fmt.Sprintf("%s/payments?apiKey=%s", h.baseURL, src.APIKey)
			resp, err := http.Get(uri)
			if err != nil {
				return err
			}
			err = json.NewDecoder(resp.Body).Decode(&p)
			if err != nil {
				return err
			}

			for _, py := range p {
				err := tracker.Record(py, stream.Stream.Name, stream.Stream.Namespace)
				if err != nil {
					return err
				}
			}
		}
	}

	tracker.State(&LastSyncTime{
		Timestamp: time.Now().UnixMilli(),
	})
	return nil
}
