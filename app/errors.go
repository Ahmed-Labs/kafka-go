package main

type ErrorCode int16

const (
	NONE                       ErrorCode = 0
	UNSUPPORTED_VERSION        ErrorCode = 35
	UNKNOWN_TOPIC_OR_PARTITION ErrorCode = 3
)
