package main

import (
	"bytes"
	"encoding/binary"
)


type FetchResponse struct {
	throttleTime int32
	errorCode ErrorCode
	sessionID int32
	responses []FetchResponseTopic
	tagBuffer byte
}

type FetchResponseTopic struct {
	topicID UUID
	partitions []FetchResponsePartition
	tagBuffer byte
}

type FetchResponsePartition struct {
	partitionIndex int32
	errorCode ErrorCode
	highWatermark int64
	lastStableOffset int64
	logStartOffset int64
	abortedTransactions []FetchResponseAbortedTransaction
	preferredReadReplica ReplicaID
	records byte
	tagBuffer byte
}

type FetchResponseAbortedTransaction struct {
	producerID int64
	firstOffset int64
	tagBuffer byte
}

func (f FetchResponse) serialize() []byte {
	out := []byte{}

	out = binary.BigEndian.AppendUint32(out, uint32(f.throttleTime))
	out = binary.BigEndian.AppendUint16(out, uint16(f.errorCode))
	out = binary.BigEndian.AppendUint32(out, uint32(f.sessionID))

	serializableElements := make([]SerializableElement, len(f.responses))
	for i, v := range f.responses {
		serializableElements[i] = v
	}
	out = append(out, encodeCustomCompactArray(serializableElements)...)

	out = append(out, f.tagBuffer)
	return out
}

func (f FetchResponseTopic) serialize() []byte {
	out := []byte{}
	out = append(out, f.topicID[:]...)

	serializableElements := make([]SerializableElement, len(f.partitions))
	for i, v := range f.partitions {
		serializableElements[i] = v
	}
	out = append(out, encodeCustomCompactArray(serializableElements)...)
	
	out = append(out, f.tagBuffer)
	return out
}

func (f FetchResponsePartition) serialize() []byte {
	out := []byte{}
	out = binary.BigEndian.AppendUint32(out, uint32(f.partitionIndex))
	out = binary.BigEndian.AppendUint16(out, uint16(f.errorCode))

	out = binary.BigEndian.AppendUint64(out, uint64(f.highWatermark))
	out = binary.BigEndian.AppendUint64(out, uint64(f.lastStableOffset))
	out = binary.BigEndian.AppendUint64(out, uint64(f.logStartOffset))

	serializableElements := make([]SerializableElement, len(f.abortedTransactions))
	for i, v := range f.abortedTransactions {
		serializableElements[i] = v
	}
	out = append(out, encodeCustomCompactArray(serializableElements)...)

	out = binary.BigEndian.AppendUint32(out, uint32(f.preferredReadReplica))

	// Compact records
	out = append(out, f.records)

	out = append(out, f.tagBuffer)
	return out
}

func (f FetchResponseAbortedTransaction) serialize() []byte {
	out := []byte{}
	out = binary.BigEndian.AppendUint64(out, uint64(f.producerID))
	out = binary.BigEndian.AppendUint64(out, uint64(f.firstOffset))

	out = append(out, f.tagBuffer)
	return out
}


func buildFetchResposne(req RequestMessage) FetchResponse {
	reqBody := req.body.(*FetchRequest)
	res :=  FetchResponse{
		throttleTime: 0,
		sessionID: reqBody.sessionID,
	}

	for _, topic := range(reqBody.topics) {
		foundTopic := getTopicByID(topic.topicID)

		err := ERR_NONE
		if foundTopic.errorCode != ERR_NONE {
			err = foundTopic.errorCode
		}

		responseTopic := FetchResponseTopic{
			topicID: topic.topicID,
		}
		for _, partition := range(topic.partitions) {
			responsePartition := FetchResponsePartition{
				partitionIndex: partition.partition,
				errorCode: err,
			}
			responseTopic.partitions = append(responseTopic.partitions, responsePartition)
		}
		res.responses = append(res.responses, responseTopic)
	}

	return res
}

// Request
type FetchRequest struct {
	maxWait int32
	minBytes int32
	maxBytes int32
	isolationLevel int8
	sessionID int32
	sessionEpoch int32
	topics []FetchRequestTopic
	forgottenTopicsData []ForgottenTopic
	rackID string
	tagBuffer byte
}

type FetchRequestTopic struct {
	topicID UUID
	partitions []FetchRequestPartition
	tagBuffer byte
}

type FetchRequestPartition struct {
	partition int32
	currentLeaderEpoch int32
	fetchOffset int64
	lastFetchedEpoch int32
	logStartOffset int64
	partitionMaxBytes int32
	tagBuffer byte
}

type ForgottenTopic struct {
	topicID UUID
	partitions int32
	tagBuffer byte
}

func (r *FetchRequest) deserialize(data []byte) {
	buf := bytes.NewBuffer(data)

	err := binary.Read(buf, binary.BigEndian, &r.maxWait)
	checkError(err)

	err = binary.Read(buf, binary.BigEndian, &r.minBytes)
	checkError(err)

	err = binary.Read(buf, binary.BigEndian, &r.maxBytes)
	checkError(err)

	err = binary.Read(buf, binary.BigEndian, &r.isolationLevel)
	checkError(err)

	err = binary.Read(buf, binary.BigEndian, &r.sessionID)
	checkError(err)

	err = binary.Read(buf, binary.BigEndian, &r.sessionEpoch)
	checkError(err)

	topics := readCustomComapctArray(buf, newFetchRequestTopic)
	for _, elem := range topics {
		if topic, ok := elem.(*FetchRequestTopic); ok {
			r.topics = append(r.topics, *topic)
		} 
	}
	
	forgottenTopics := readCustomComapctArray(buf, newForgottenTopic)
	for _, elem := range forgottenTopics {
		if topic, ok := elem.(*ForgottenTopic); ok {
			r.forgottenTopicsData = append(r.forgottenTopicsData, *topic)
		} 
	}

	r.rackID = readComapctString(buf)

	err = binary.Read(buf, binary.BigEndian, &r.tagBuffer)
	checkError(err)
}

func newFetchRequestTopic() CompactArrayElement {
	return &FetchRequestTopic{}
}

func (t *FetchRequestTopic) deserialize(buf *bytes.Buffer) {
	err := binary.Read(buf, binary.BigEndian, &t.topicID)
	checkError(err)
	
	topicPartitions := readCustomComapctArray(buf, newFetchRequestPartition)
	for _, elem := range topicPartitions {
		if partition, ok := elem.(*FetchRequestPartition); ok {
			t.partitions = append(t.partitions, *partition)
		} 
	}

	err = binary.Read(buf, binary.BigEndian, &t.tagBuffer)
	checkError(err)
}

func newFetchRequestPartition() CompactArrayElement {
	return &FetchRequestPartition{}
}

func (p *FetchRequestPartition) deserialize(buf *bytes.Buffer) {
	err := binary.Read(buf, binary.BigEndian, &p.partition)
	checkError(err)

	err = binary.Read(buf, binary.BigEndian, &p.currentLeaderEpoch)
	checkError(err)

	err = binary.Read(buf, binary.BigEndian, &p.fetchOffset)
	checkError(err)

	err = binary.Read(buf, binary.BigEndian, &p.lastFetchedEpoch)
	checkError(err)

	err = binary.Read(buf, binary.BigEndian, &p.logStartOffset)
	checkError(err)

	err = binary.Read(buf, binary.BigEndian, &p.partitionMaxBytes)
	checkError(err)

	err = binary.Read(buf, binary.BigEndian, &p.tagBuffer)
	checkError(err)
}

func newForgottenTopic() CompactArrayElement {
	return &ForgottenTopic{}
}

func (f *ForgottenTopic) deserialize(buf *bytes.Buffer) {
	err := binary.Read(buf, binary.BigEndian, &f.topicID)
	checkError(err)

	err = binary.Read(buf, binary.BigEndian, &f.partitions)
	checkError(err)

	err = binary.Read(buf, binary.BigEndian, &f.tagBuffer)
	checkError(err)
}