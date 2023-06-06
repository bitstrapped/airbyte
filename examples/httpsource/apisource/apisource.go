package apisource

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	airbyte "github.com/kollalabs/airbyte-go"
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
	if err := logTracker.Log(airbyte.LogLevelInfo, "Running Spec"); err != nil {
		return nil, err
	}
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
	if err := logTracker.Log(airbyte.LogLevelDebug, "validating api connection"); err != nil {
		return err
	}
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

	return resp.Body.Close()
}

func (h APISource) Discover(srcCfgPath string, logTracker airbyte.LogTracker) (json.RawMessage, error) {
	var srcCfg HTTPConfig
	err := airbyte.UnmarshalFromPath(srcCfgPath, &srcCfg)
	if err != nil {
		return nil, err
	}

	message := `{
		"type": "CATALOG",
		"catalog": {
		  "streams": [
			{
			  "name": "Company",
			  "json_schema": {
				"$schema": "http://json-schema.org/draft-07/schema#",
				"title": "CompanyRet",
				"type": "object",
				"properties": {
				  "IsSampleCompany": {"type": "boolean"},
				  "CompanyName": {"type": "string"},
				  "Address": {
					"type": "object",
					"properties": {
					  "Street": {"type": "string"},
					  "CityStateZIP": {"type": "string"},
					  "Misc1": {"type": "string"},
					  "Misc2": {"type": "string"},
					  "Misc3": {"type": "string"}
					}
				  },
				  "QuickBooksCompanyFile": {"type": "string"},
				  "StoreNumber": {"type": "integer"},
				  "StoreCode": {"type": "integer"},
				  "Store": {
					"type": "object",
					"properties": {
					  "StoreNumber": {"type": "integer"},
					  "StoreCode": {"type": "integer"},
					  "StoreName": {"type": "string"},
					  "Address": {
						"type": "object",
						"properties": {
						  "Street": {"type": "string"},
						  "CityStateZIP": {"type": "string"},
						  "Misc1": {"type": "string"},
						  "Misc2": {"type": "string"},
						  "Misc3": {"type": "string"}
						}
					  }
					}
				  },
				  "PriceLevel1": {
					"type": "object",
					"properties": {
					  "Name": {"type": "string"},
					  "Markdown": {"type": "number"}
					}
				  },
				  "PriceLevel2": {
					"type": "object",
					"properties": {
					  "Name": {"type": "string"},
					  "Markdown": {"type": "number"}
					}
				  },
				  "PriceLevel3": {
					"type": "object",
					"properties": {
					  "Name": {"type": "string"},
					  "Markdown": {"type": "number"}
					}
				  },
				  "PriceLevel4": {
					"type": "object",
					"properties": {
					  "Name": {"type": "string"},
					  "Markdown": {"type": "number"}
					}
				  },
				  "PriceLevel5": {
					"type": "object",
					"properties": {
					  "Name": {"type": "string"},
					  "Markdown": {"type": "number"}
				   }
				},
				"PurchaseOrderStatusInfo": {
				  "type": "object",
				  "properties": {
					"OrderStatusData": {
					  "type": "array",
					  "items": {
						"type": "object",
						"properties": {
						  "StatusType": {"type": "string", "enum": ["Open", "Closed", "Custom"]},
						  "StatusDescription": {"type": "string"}
						}
					  }
					}
				  }
				},
				"LayawayStatusInfo": {
				  "type": "object",
				  "properties": {
					"OrderStatusData": {
					  "type": "array",
					  "items": {
						"type": "object",
						"properties": {
						  "StatusType": {"type": "string", "enum": ["Open", "Closed", "Custom"]},
						  "StatusDescription": {"type": "string"}
						}
					  }
					}
				  }
				},
				"SalesOrderStatusInfo": {
				  "type": "object",
				  "properties": {
					"OrderStatusData": {
					  "type": "array",
					  "items": {
						"type": "object",
						"properties": {
						  "StatusType": {"type": "string", "enum": ["Open", "Closed", "Custom"]},
						  "StatusDescription": {"type": "string"}
						}
					  }
					}
				  }
				},
				"WorkOrderStatusInfo": {
				  "type": "object",
				  "properties": {
					"OrderStatusData": {
					  "type": "array",
					  "items": {
						"type": "object",
						"properties": {
						  "StatusType": {"type": "string", "enum": ["Open", "Closed", "Custom"]},
						  "StatusDescription": {"type": "string"}
						}
					  }
					}
				  }
				},
				"IsUsingUnitsOfMeasure": {"type": "boolean"},
				"IsUsingIntegratedShipping": {"type": "boolean"},
				"ShippingProvider": {"type": "string"},
				"TaxRecord": {
				  "type": "array",
				  "items": {
					"type": "object",
					"properties": {
					  "TaxCategoryListID": {"type": "string"},
					  "TaxCategory": {"type": "string"},
					  "POSTaxCodeListID": {"type": "string"},
					  "POSTaxCode": {"type": "string"},
					  "TaxPercent": {"type": "number"},
					  "TaxRate": {
						"type": "array",
						"items": {
						  "type": "object",
						  "properties": {
							"TaxPercent": {"type": "number"},
							"TaxRateName": {"type": "string"},
							"TaxAgency": {"type": "string"},
							"TaxLowRange": {"type": "number"},
							"TaxHighRange": {"type": "number"},
							"IsTaxAppliedOnlyWithinRange": {"type": "boolean"},
							"QBTaxGroup": {"type": "string"}
						  }
						}
					  },
					  "QBTaxGroup": {"type": "string"},
					  "QBTaxCode": {"type": "string"}
					}
				  }
				},
				"DataExtRet": {
				  "type": "object",
				  "properties": {
					"OwnerID": {"type": "string"},
					"DataExtName": {"type": "string"},
					"DataExtType": {"type": "string", "enum": ["INTTYPE", "AMTTYPE", "PRICETYPE", "QUANTYPE", "PERCENTTYPE", "DATETIMETYPE", "STR255TYPE", "STR1024TYPE"]},
					"DataExtValue": {"type": "string"}
				  }
				}
			  }
			},
			  "supported_sync_modes": [
				"full_refresh"
			  ],
			  "namespace": "kolla"
			}
		  ]
		}
	  }`

	return json.RawMessage(message), nil
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
	if err := tracker.Log(airbyte.LogLevelInfo, "Running read"); err != nil {
		return err
	}
	var src HTTPConfig
	err := airbyte.UnmarshalFromPath(sourceCfgPath, &src)
	if err != nil {
		return err
	}

	// see if there is a last sync
	var st LastSyncTime
	_ = airbyte.UnmarshalFromPath(sourceCfgPath, &st)
	if st.Timestamp <= 0 {
		st.Timestamp = -1
	}

	for _, stream := range configuredCat.Streams {
		if stream.Stream.Name == "users" {
			var u []User
			uri := fmt.Sprintf("https://api.bistrapped.com/users?apiKey=%s", src.APIKey)
			if err := httpGet(uri, &u); err != nil {
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
			if err := httpGet(uri, &p); err != nil {
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

	return tracker.State(&LastSyncTime{
		Timestamp: time.Now().UnixMilli(),
	})
}

func httpGet(uri string, v interface{}) error {
	resp, err := http.Get(uri)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(v)
}
