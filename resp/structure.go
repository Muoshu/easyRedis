package resp

import (
	"strconv"
)

// this file implements data structure for resp

var CRLF = "\r\n"

type RedisData interface {
	ToBytes() []byte  // return resp transfer format data
	ByteData() []byte // return byte data
}

type StringData struct {
	data string
}

type BulkData struct {
	data []byte
}

type IntData struct {
	data int64
}

type Float64Data struct {
	data float64
}

type ErrorData struct {
	data string
}

type ArrayData struct {
	data []RedisData
}

type PlainData struct {
	data string
}

func NewBulkData(data []byte) *BulkData {
	return &BulkData{
		data: data,
	}
}

func (b *BulkData) ToBytes() []byte {
	if b.data == nil {
		return []byte("$-1" + CRLF)
	}
	return []byte("$" + strconv.Itoa(len(b.data)) + CRLF + string(b.data) + CRLF)
}

func (b *BulkData) Data() []byte {
	return b.data
}

func (b *BulkData) ByteData() []byte {
	return b.data
}

func NewStringData(data string) *StringData {
	return &StringData{
		data: data,
	}
}

func (s *StringData) ToBytes() []byte {
	return []byte("+" + s.data + CRLF)
}

func (s *StringData) ByteData() []byte {
	return []byte(s.data)
}

func (s *StringData) Data() string {
	return s.data
}

func NewIntData(data int64) *IntData {
	return &IntData{
		data: data,
	}
}

func (i *IntData) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(i.data, 10) + CRLF)
}

func (i *IntData) ByteData() []byte {
	return []byte(strconv.FormatInt(i.data, 10))
}

func (i *IntData) Data() int64 {
	return i.data
}

func NewFloat64Data(data float64) *Float64Data {
	return &Float64Data{
		data: data,
	}
}

func (f *Float64Data) ToBytes() []byte {
	return []byte(":" + strconv.FormatFloat(f.data, 'f', -1, 64) + CRLF)
}

func (f *Float64Data) ByteData() []byte {
	return []byte(strconv.FormatFloat(f.data, 'f', -1, 64))
}

func (f *Float64Data) Data() float64 {
	return f.data
}

func NewErrorData(data string) *ErrorData {
	return &ErrorData{
		data: data,
	}
}

func (e *ErrorData) ToBytes() []byte {
	return []byte("-" + e.data + CRLF)
}
func (e *ErrorData) ByteData() []byte {
	return []byte(e.data)
}
func (e *ErrorData) Error() string {
	return e.data
}

func NewArrayData(data []RedisData) *ArrayData {
	return &ArrayData{
		data: data,
	}
}

func (a *ArrayData) ToBytes() []byte {
	if a.data == nil {
		return []byte("*-1" + CRLF)
	}
	res := []byte("*" + strconv.Itoa(len(a.data)) + CRLF)
	for _, v := range a.data {
		res = append(res, v.ToBytes()...)
	}
	return res
}

func (a *ArrayData) Data() []RedisData {
	return a.data
}

func (a *ArrayData) ToCommand() [][]byte {
	res := make([][]byte, 0)
	for _, v := range a.data {
		res = append(res, v.ByteData())
	}
	return res
}

func (a *ArrayData) ByteData() []byte {
	res := make([]byte, 0)
	for _, v := range a.data {
		res = append(res, v.ByteData()...)
	}
	return res
}

func NewPlainData(data string) *PlainData {
	return &PlainData{
		data: data,
	}
}

func (p *PlainData) ToBytes() []byte {
	return []byte(p.data + CRLF)
}
func (p *PlainData) Data() string {
	return p.data
}
func (p *PlainData) ByteData() []byte {
	return []byte(p.data)
}
