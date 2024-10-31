package main

var (
	GlobalTopics                  map[string]Topic
	DEFAULT_TOPIC_ID              = [16]byte{0}
	DEFAULT_AUTHORIZED_OPERATIONS = [4]byte{0, 0, 0x0d, 0xf8}
)

type Topic struct {
	ID         [16]byte
	Name       string
	Partitions []int
}
