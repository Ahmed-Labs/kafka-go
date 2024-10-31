package main

import "encoding/binary"


var supportedApiKeys = []int16{18, 19}

const (
	minVersion int16 = 0
	maxVersion int16 = 4
)

type ApiVersion struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
}

type ApiVersionsResponse struct {
	apiVersions []ApiVersion
}

func (r ApiVersionsResponse) serialize() []byte {
	var body []byte

	numApiVersions := len(r.apiVersions)
	body = append(body, byte(numApiVersions+1))

	for _, version := range r.apiVersions {
		body = binary.BigEndian.AppendUint16(body, uint16(version.ApiKey))
		body = binary.BigEndian.AppendUint16(body, uint16(version.MinVersion))
		body = binary.BigEndian.AppendUint16(body, uint16(version.MaxVersion))
		body = append(body, 0) // Compact array tag buffer
	}
	return body
}