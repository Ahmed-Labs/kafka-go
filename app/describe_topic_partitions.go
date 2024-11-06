package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// Response
type DescribeTopicPartitionsResponse struct {
	TopicPartitions []TopicPartitionDetails
}

type TopicPartitionDetails struct {
	errorCode            ErrorCode
	topicName            string
	topicID              [16]byte
	isInternal           bool
	partitions           []int
	authorizedOperations [4]byte
}

func (r DescribeTopicPartitionsResponse) serialize() []byte {
	res := []byte{}

	// Throttle Time
	res = append(res, []byte{0, 0, 0, 0}...)
	// Topics Array
	arrayLength := len(r.TopicPartitions) + 1
	res = append(res, byte(arrayLength))
	fmt.Println("Array len:", byte(arrayLength))
	fmt.Println("Current:", res)

	for _, details := range r.TopicPartitions {
		// Error code
		res = binary.BigEndian.AppendUint16(res, uint16(details.errorCode))
		// Topic Name
		topicName := encodeCompactString(details.topicName)
		res = append(res, topicName...)
		// Topic ID
		res = append(res, details.topicID[:]...)
		// Is Internal
		var internal byte = 0x00
		if details.isInternal {
			internal = 0x11
		}
		res = append(res, internal)
		// Partitions Array (ADD COMPACT ARRAY ENCODER lATER!!)
		arrayLength := len(details.partitions) + 1
		res = append(res, byte(arrayLength))
		// Topic Authorized Operations
		res = append(res, details.authorizedOperations[:]...)
		// Tag Buffer
		var tagBuffer byte = 0
		res = append(res, tagBuffer)
	}
	// Next Cursor
	var cursor byte = 0xff // (null)
	res = append(res, cursor)
	// Tag Buffer
	var tagBuffer byte = 0
	res = append(res, tagBuffer)

	return res
}

func buildDescribeTopicPartitionsResponse(req RequestMessage) DescribeTopicPartitionsResponse {
	reqBody := req.body.(*DescribeTopicPartitionsRequest)
	response := DescribeTopicPartitionsResponse{}

	for _, topicName := range reqBody.TopicNames {
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

// Request
type DescribeTopicPartitionsRequest struct {
	TopicNames []string
}

func (r *DescribeTopicPartitionsRequest) deserialize(data []byte) {
	buf := bytes.NewBuffer(data)
	r.TopicNames = []string{}

	var lengthByte byte
	binary.Read(buf, binary.BigEndian, &lengthByte)
	length := max(int(lengthByte)-1, 0)
	if length == 0 {
		return
	}

	for range length {
		var topicNameLengthByte byte
		binary.Read(buf, binary.BigEndian, &topicNameLengthByte)
		topicNameLength := max(int(topicNameLengthByte)-1, 0)

		topicName := make([]byte, topicNameLength)
		buf.Read(topicName)

		r.TopicNames = append(r.TopicNames, string(topicName))
		var topicTagBuffer byte
		binary.Read(buf, binary.BigEndian, &topicTagBuffer)
	}

	responsePartitionLimit := make([]byte, 4)
	buf.Read(responsePartitionLimit)

	var cursor, tagBuffer byte
	binary.Read(buf, binary.BigEndian, &cursor)
	binary.Read(buf, binary.BigEndian, &tagBuffer)
}