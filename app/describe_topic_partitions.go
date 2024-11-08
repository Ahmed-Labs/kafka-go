package main

import (
	"bytes"
	"encoding/binary"
)

// Response
type DescribeTopicPartitionsResponse struct {
	throttleTime int32
	Topics       []Topic
	nextCursor   byte
	tagBuffer    byte
}

func (r DescribeTopicPartitionsResponse) serialize() []byte {
	res := []byte{}

	// Throttle Time
	res = binary.BigEndian.AppendUint32(res, uint32(r.throttleTime))

	// Topics Array
	arrayLength := len(r.Topics) + 1
	res = append(res, byte(arrayLength))

	for _, details := range r.Topics {
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

		// Partitions Array
		arrayLength := len(details.partitions) + 1
		res = append(res, byte(arrayLength))

		for _, partition := range details.partitions {
			res = append(res, partition.serialize()...)
		}

		// Topic Authorized Operations
		res = append(res, details.authorizedOperations[:]...)
		// Tag Buffer
		res = append(res, details.tagBuffer)
	}
	// Next Cursor
	res = append(res, r.nextCursor)
	// Tag Buffer
	res = append(res, r.tagBuffer)

	return res
}

func buildDescribeTopicPartitionsResponse(req RequestMessage) DescribeTopicPartitionsResponse {
	reqBody := req.body.(*DescribeTopicPartitionsRequest)
	response := DescribeTopicPartitionsResponse{
		throttleTime: 0,
		nextCursor:   0xff,
		tagBuffer:    0,
	}

	for _, topicName := range reqBody.TopicNames {
		topic := getTopicByName(topicName)
		response.Topics = append(response.Topics, topic)
	}

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

	var responsePartitionLimit int32
	binary.Read(buf, binary.BigEndian, &responsePartitionLimit)

	var cursor, tagBuffer byte
	binary.Read(buf, binary.BigEndian, &cursor)
	binary.Read(buf, binary.BigEndian, &tagBuffer)
}
