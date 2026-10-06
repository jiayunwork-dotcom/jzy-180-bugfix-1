package httpapi

import "encoding/json"

func jsonUnmarshalBytes(raw []byte, v any) error { return json.Unmarshal(raw, v) }
