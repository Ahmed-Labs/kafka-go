package main

type ErrorCode int16

const (
	ERR_NONE                       ErrorCode = 0
	ERR_UNKNOWN_TOPIC_OR_PARTITION ErrorCode = 3
	ERR_UNSUPPORTED_VERSION        ErrorCode = 35
)
