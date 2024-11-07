package main

import (
	"bytes"
	"reflect"
	"testing"
)

func Test_readCompactArray(t *testing.T) {
	type args struct {
		buf *bytes.Buffer
	}

	type test[T any] struct {
		name string
		args args
		want []T
	}
	intTests := []test[int32]{
		// TODO: Add test cases.
		{
			name: "first",
			args: args{
				buf: bytes.NewBuffer([]byte{3, 0, 0, 0, 4, 0, 0, 0, 2}),
			},
			want: []int32{4, 2},
		},
	}
	uuidTests := []test[UUID]{
		// TODO: Add test cases.
		{
			name: "first",
			args: args{
				buf: bytes.NewBuffer([]byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 6}),
			},
			want: []UUID{{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 6}},
		},
	}
	for _, tt := range intTests {
		t.Run(tt.name, func(t *testing.T) {
			if got := readCompactArray[int32](tt.args.buf); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("readCompactArray() = %v, want %v", got, tt.want)
			}
		})
	}

	for _, tt := range uuidTests {
		t.Run(tt.name, func(t *testing.T) {
			if got := readCompactArray[UUID](tt.args.buf); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("readCompactArray() = %v, want %v", got, tt.want)
			}
		})
	}

}

func Test_decodeSignedVarint(t *testing.T) {
	type args struct {
		n int
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "reading -1",
			args: args{n: 0x01},
			want: -1,
		},
		{
			name: "reading signed varint",
			args: args{n: 0x30},
			want: 24,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := decodeSignedVarint(tt.args.n); got != tt.want {
				t.Errorf("decodeSignedVarint() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_readSignedVarint(t *testing.T) {
	type args struct {
		buf *bytes.Buffer
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "read variable sized integer",
			args: args{
				buf: bytes.NewBuffer([]byte{0x82, 0x01}),
			},
			want: 65,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := readSignedVarint(tt.args.buf); got != tt.want {
				t.Errorf("readSignedVarint() = %v, want %v", got, tt.want)
			}
		})
	}
}
