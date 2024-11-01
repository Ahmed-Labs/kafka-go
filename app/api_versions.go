package main

import "encoding/binary"

type ApiKey int16

const (
	API_VERSIONS              ApiKey = 18
	DESCRIBE_TOPIC_PARTITIONS ApiKey = 75
)

type ApiVersion struct {
	ApiKey     ApiKey
	MinVersion int16
	MaxVersion int16
}

type ApiVersionsResponse struct {
	apiVersions []ApiVersion
	errorCode   ErrorCode
}

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

	// Set error code
	body = binary.BigEndian.AppendUint16(body, uint16(r.errorCode))

	numApiVersions := len(r.apiVersions) + 1
	body = append(body, byte(numApiVersions))

	for _, version := range r.apiVersions {
		body = binary.BigEndian.AppendUint16(body, uint16(version.ApiKey))
		body = binary.BigEndian.AppendUint16(body, uint16(version.MinVersion))
		body = binary.BigEndian.AppendUint16(body, uint16(version.MaxVersion))

		// Compact array tag buffer
		body = append(body, 0)
	}

	// Throttle time
	body = append(body, []byte{0, 0, 0, 0}...)

	// Tag buffer
	body = append(body, 0)
	return body
}

func buildApiVersionsResponse(req RequestMessage) ApiVersionsResponse {
	var err ErrorCode = NONE
	v := req.header.requestApiVersion
	apiKey := req.header.requestApiKey

	for _, version := range SupportedApiVersions {
		if version.ApiKey != apiKey {
			continue
		}
		if !(v >= version.MinVersion && v <= version.MaxVersion) {
			err = UNSUPPORTED_VERSION
			break
		}
	}
	return ApiVersionsResponse{
		apiVersions: SupportedApiVersions,
		errorCode:   err,
	}
}
