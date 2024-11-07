package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
)

type RecordType byte

type RecordBatchInfo struct {
	recordsLength int
}
type Records struct {
	TopicRecords     []TopicRecord
	PartitionRecords []PartitionRecord
}

const (
	TOPIC_RECORD     RecordType = 2
	PARTITION_RECORD RecordType = 3
)

type Record interface{}

type TopicRecord struct {
	version   byte
	topicName string
	topicUUID UUID
}

type PartitionRecord struct {
	version          byte
	partitionID      int32
	topicUUID        UUID
	replicaNodes     []ReplicaID
	isrNodes         []ReplicaID
	removingReplicas []ReplicaID
	addingReplicas   []ReplicaID
	leader           ReplicaID
	leaderEpoch      int32
	partitionEpoch   int32
	directories      []UUID
}

func getRecords() Records {
	data, err := os.ReadFile("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log")
	checkError(err)

	buf := bytes.NewBuffer(data)
	records := Records{}

	for buf.Len() > 0 {
		batchBuf := getRecordBatchBuffer(buf)
		info := readRecordBatchInfo(batchBuf)

		for range info.recordsLength {
			record := readRecord(batchBuf)
			switch r := record.(type) {
			case TopicRecord:
				records.TopicRecords = append(records.TopicRecords, r)
			case PartitionRecord:
				records.PartitionRecords = append(records.PartitionRecords, r)
			}
		}
	}
	fmt.Printf("Records: %+v\n", records)
	return records
}

func getRecordBatchBuffer(buf *bytes.Buffer) *bytes.Buffer {
	var baseOffset int64
	err := binary.Read(buf, binary.BigEndian, &baseOffset)
	checkError(err)

	var batchLengthBytes int32
	err = binary.Read(buf, binary.BigEndian, &batchLengthBytes)
	checkError(err)

	out := make([]byte, batchLengthBytes)
	buf.Read(out)
	return bytes.NewBuffer(out)
}

func readRecordBatchInfo(buf *bytes.Buffer) (info RecordBatchInfo) {
	var partitionLeader int32
	err := binary.Read(buf, binary.BigEndian, &partitionLeader)
	checkError(err)

	var magicByte byte
	err = binary.Read(buf, binary.BigEndian, &magicByte)
	checkError(err)

	var crc int32
	err = binary.Read(buf, binary.BigEndian, &crc)
	checkError(err)

	var attributes int16
	err = binary.Read(buf, binary.BigEndian, &attributes)
	checkError(err)

	var lastOffsetDelta int32
	err = binary.Read(buf, binary.BigEndian, &lastOffsetDelta)
	checkError(err)

	var baseTimestamp int64
	err = binary.Read(buf, binary.BigEndian, &baseTimestamp)
	checkError(err)

	var maxTimestamp int64
	err = binary.Read(buf, binary.BigEndian, &maxTimestamp)
	checkError(err)

	var producerID int64
	err = binary.Read(buf, binary.BigEndian, &producerID)
	checkError(err)

	var producerEpoch int16
	err = binary.Read(buf, binary.BigEndian, &producerEpoch)
	checkError(err)

	var baseSequence int32
	err = binary.Read(buf, binary.BigEndian, &baseSequence)
	checkError(err)

	var recordsLength int32
	err = binary.Read(buf, binary.BigEndian, &recordsLength)
	checkError(err)

	info.recordsLength = int(recordsLength)
	return info
}

func readRecord(inBuf *bytes.Buffer) Record {
	// Get length of record
	recordLength := readSignedVarint(inBuf)

	// Read record from inBuf and transfer it to new buf
	recordData := make([]byte, recordLength)
	err := binary.Read(inBuf, binary.BigEndian, &recordData)
	checkError(err)

	if recordLength <= 5 {
		return nil
	}

	buf := bytes.NewBuffer(recordData)

	// Read record fields from buf
	var attributes byte
	err = binary.Read(buf, binary.BigEndian, &attributes)
	checkError(err)

	var timestampDelta byte
	err = binary.Read(buf, binary.BigEndian, &timestampDelta)
	checkError(err)

	var offsetDelta byte
	err = binary.Read(buf, binary.BigEndian, &offsetDelta)
	checkError(err)

	keyLength := readSignedVarint(buf)

	if keyLength > 0 {
		key := make([]byte, keyLength)
		err = binary.Read(buf, binary.BigEndian, &key)
		checkError(err)
	}

	valueLength := readSignedVarint(buf)

	if valueLength > 0 {
		valueBytes := make([]byte, valueLength)
		err = binary.Read(buf, binary.BigEndian, &valueBytes)
		checkError(err)

		valueBuffer := bytes.NewBuffer(valueBytes)

		var frameVersion byte
		err = binary.Read(valueBuffer, binary.BigEndian, &frameVersion)
		checkError(err)

		var recordType RecordType
		err = binary.Read(valueBuffer, binary.BigEndian, &recordType)
		checkError(err)

		switch recordType {
		case TOPIC_RECORD:
			return readTopicRecord(valueBuffer)
		case PARTITION_RECORD:
			return readPartitionRecord(valueBuffer)
		}
	}
	return nil
}

func readTopicRecord(buf *bytes.Buffer) TopicRecord {
	topicRecord := TopicRecord{}

	err := binary.Read(buf, binary.BigEndian, &topicRecord.version)
	checkError(err)

	var nameLengthByte byte
	err = binary.Read(buf, binary.BigEndian, &nameLengthByte)
	checkError(err)
	nameLength := int(nameLengthByte) - 1

	if nameLength > 0 {
		topicName := make([]byte, nameLength)
		err = binary.Read(buf, binary.BigEndian, &topicName)
		checkError(err)

		topicRecord.topicName = string(topicName)
	}

	var topicUUID UUID
	err = binary.Read(buf, binary.BigEndian, &topicUUID)
	checkError(err)

	topicRecord.topicUUID = topicUUID

	var taggedFields byte
	err = binary.Read(buf, binary.BigEndian, &taggedFields)
	checkError(err)

	return topicRecord
}

func readPartitionRecord(buf *bytes.Buffer) PartitionRecord {
	partitionRecord := PartitionRecord{}

	err := binary.Read(buf, binary.BigEndian, &partitionRecord.version)
	checkError(err)

	err = binary.Read(buf, binary.BigEndian, &partitionRecord.partitionID)
	checkError(err)

	err = binary.Read(buf, binary.BigEndian, &partitionRecord.topicUUID)
	checkError(err)

	partitionRecord.replicaNodes = readCompactArray[ReplicaID](buf)
	partitionRecord.isrNodes = readCompactArray[ReplicaID](buf)
	partitionRecord.removingReplicas = readCompactArray[ReplicaID](buf)
	partitionRecord.addingReplicas = readCompactArray[ReplicaID](buf)

	err = binary.Read(buf, binary.BigEndian, &partitionRecord.leader)
	checkError(err)

	err = binary.Read(buf, binary.BigEndian, &partitionRecord.leaderEpoch)
	checkError(err)

	err = binary.Read(buf, binary.BigEndian, &partitionRecord.partitionEpoch)
	checkError(err)

	partitionRecord.directories = readCompactArray[UUID](buf)

	return partitionRecord
}
