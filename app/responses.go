package main

import (
	"encoding/binary"
	"fmt"
	"net"
)

type ResponseHeader struct {
	correlationID int32
}

type ResponseMessage struct {
	header    ResponseHeader
	body      SerializableResponse
	errorCode ErrorCode
}

type SerializableResponse interface {
	serialize() []byte
}

func NewResponseBody(req RequestMessage) SerializableResponse {
	apiKey := req.header.requestApiKey
	requestBody := req.body
	requestHeader := req.header

	switch apiKey {
	case API_VERSIONS:
		var err ErrorCode = NONE
		v := requestHeader.requestApiVersion

		for _, version := range(SupportedApiVersions) {
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
			errorCode: err,
		}

	case DESCRIBE_TOPIC_PARTITIONS:
		reqBody := requestBody.(*DescribeTopicPartitionsRequest)
		response := DescribeTopicPartitionsResponse{}

		for _, topicName := range reqBody.TopicNames {
			fmt.Println("Topic name: ", topicName, len(topicName))
			details := TopicPartitionDetails{
				errorCode:            UNKNOWN_TOPIC_OR_PARTITION,
				topicName:            topicName,
				topicID:              DEFAULT_TOPIC_ID,
				isInternal:           false,
				partitions:           []int{},
				authorizedOperations: DEFAULT_AUTHORIZED_OPERATIONS,
			}

			if foundTopic, ok := GlobalTopics[topicName]; ok {
				details.errorCode = NONE
				details.topicID = foundTopic.ID
				details.partitions = foundTopic.Partitions
			}
			response.TopicPartitions = append(response.TopicPartitions, details)
		}
		fmt.Println("Response topic partitions: ", response.TopicPartitions)
		return response
	}

	return nil
}

func (rs ResponseHeader) serialize() []byte {
	header := make([]byte, 4) 
	binary.BigEndian.PutUint32(header, uint32(rs.correlationID))

	// Extra byte for tag buffer
	// header = append(header, 0)
	return header
}

func (r ResponseMessage) serialize() []byte {
	serializedHeader := r.header.serialize()
	serializedBody := []byte{}

	if r.body != nil {
		body := r.body.serialize()
		serializedBody = append(serializedBody, body...)
	}

	messageSize := len(serializedHeader) + len(serializedBody)
	message := make([]byte, 4+messageSize) // messageSize itself is 4 bytes

	fmt.Println("Header", serializedHeader)
	fmt.Println("Body:", serializedBody)

	binary.BigEndian.PutUint32(message, uint32(messageSize))
	copy(message[4:], serializedHeader)
	copy(message[4+len(serializedHeader):], serializedBody)

	return message
}

func sendResponse(conn net.Conn, responseMessage ResponseMessage) {
	serializedMsg := responseMessage.serialize()
	_, err := conn.Write(serializedMsg)
	checkError(err)
	fmt.Println("Sent:", serializedMsg)
}
