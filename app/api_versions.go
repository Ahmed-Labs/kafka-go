package main

import "encoding/binary"

type ApiVersion struct {
	ApiKey     ApiKey
	MinVersion int16
	MaxVersion int16
}

type ApiVersionsResponse struct {
	apiVersions []ApiVersion
}

type ApiKey int16

const (
	API_VERSIONS              ApiKey = 18
	DESCRIBE_TOPIC_PARTITIONS ApiKey = 75
)

var SupportedApiVersions = []ApiVersion{
	{
		ApiKey:     API_VERSIONS,
		MinVersion: 0,
		MaxVersion: 4,
	},
	{
		ApiKey:     DESCRIBE_TOPIC_PARTITIONS,
		MinVersion: 0,
		MaxVersion: 0,
	},
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