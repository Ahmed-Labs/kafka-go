package main

import (
	"encoding/binary"
	"fmt"
)

var (
	TopicNameToID                 = map[string]UUID{}
	DEFAULT_TOPIC_ID              = UUID{0}
	DEFAULT_AUTHORIZED_OPERATIONS = [4]byte{0, 0, 0x0d, 0xf8}
)

type UUID [16]byte

type Topic struct {
	errorCode            ErrorCode
	topicName            string
	topicID              UUID
	isInternal           bool
	partitions           []Partition
	authorizedOperations [4]byte
	tagBuffer            byte
}

type Partition struct {
	errorCode              ErrorCode
	partitionIndex         int32
	leaderID               ReplicaID
	leaderEpoch            int32
	replicaNodes           []ReplicaID
	isrNodes               []ReplicaID
	eligibleLeaderReplicas []ReplicaID
	lastKnownELR           []ReplicaID
	offlineReplicas        []ReplicaID
	tagBuffer              byte
}

type ReplicaID int32

func getTopic(topicName string) (topic Topic) {
	ID, err := getTopicID(topicName)

	if err != nil {
		topic.errorCode = ERR_UNKNOWN_TOPIC_OR_PARTITION
	} else {
		topic.partitions = getTopicPartitions(ID)
	}

	topic.topicName = topicName
	topic.topicID = ID
	topic.isInternal = false
	topic.authorizedOperations = DEFAULT_AUTHORIZED_OPERATIONS
	topic.tagBuffer = 0

	return topic
}

func getTopicPartitions(topicID UUID) []Partition {
	records := getRecords()
	partitions := []Partition{}
	var currIdx int32

	for _, record := range records.PartitionRecords {
		if record.topicUUID == topicID {
			partition := Partition{
				errorCode:              0,
				partitionIndex:         currIdx,
				leaderID:               record.leader,
				leaderEpoch:            record.leaderEpoch,
				replicaNodes:           record.replicaNodes,
				isrNodes:               record.isrNodes,
				eligibleLeaderReplicas: []ReplicaID{},
				lastKnownELR:           []ReplicaID{},
				offlineReplicas:        []ReplicaID{},
				tagBuffer:              0,
			}
			partitions = append(partitions, partition)
			currIdx++
		}
	}
	return partitions
}

func getTopicID(topicName string) (UUID, error) {
	if _, ok := TopicNameToID[topicName]; ok {
		return TopicNameToID[topicName], nil
	} else {
		records := getRecords()

		for _, topicRecord := range records.TopicRecords {
			name := topicRecord.topicName
			ID := topicRecord.topicUUID
			TopicNameToID[name] = ID
		}
	}

	if _, ok := TopicNameToID[topicName]; !ok {
		return UUID{}, fmt.Errorf("topic not found in topic records")
	}

	return TopicNameToID[topicName], nil
}

func (p Partition) serialize() []byte {
	res := []byte{}

	res = binary.BigEndian.AppendUint16(res, uint16(p.errorCode))
	res = binary.BigEndian.AppendUint32(res, uint32(p.partitionIndex))
	res = binary.BigEndian.AppendUint32(res, uint32(p.leaderID))
	res = binary.BigEndian.AppendUint32(res, uint32(p.leaderEpoch))

	replicaNodes := encodeCompactArray(p.replicaNodes, binary.BigEndian.AppendUint32)
	res = append(res, replicaNodes...)

	isrNodes := encodeCompactArray(p.isrNodes, binary.BigEndian.AppendUint32)
	res = append(res, isrNodes...)

	eligibleLeaders := encodeCompactArray(p.eligibleLeaderReplicas, binary.BigEndian.AppendUint32)
	res = append(res, eligibleLeaders...)

	lastKnownELR := encodeCompactArray(p.lastKnownELR, binary.BigEndian.AppendUint32)
	res = append(res, lastKnownELR...)

	offlineReplicas := encodeCompactArray(p.offlineReplicas, binary.BigEndian.AppendUint32)
	res = append(res, offlineReplicas...)

	res = append(res, p.tagBuffer)

	return res
}
