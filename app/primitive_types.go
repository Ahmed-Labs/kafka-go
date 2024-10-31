package main

func encodeCompactString(s string) []byte {
	encoded := []byte{}
	strLen := len(s) + 1
	encoded = append(encoded, byte(strLen))
	encoded = append(encoded, []byte(s)...)

	return encoded
}
