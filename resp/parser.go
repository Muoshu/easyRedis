package resp

import (
	"bufio"
	"easyRedis/logger"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// resp package for parsing redis serialization protocol.
// Check https://redis.io/docs/reference/protocol-spec/ for the protocol details.

type ParseRedis struct {
	Data RedisData
	Err  error
}

type readState struct {
	bulkLen   int64
	arrayLen  int
	multiLine bool
	arrayData *ArrayData
	inArray   bool
}

func ParseStream(reader io.Reader) <-chan *ParseRedis {
	ch := make(chan *ParseRedis)
	go parse(reader, ch)
	return ch
}

func parse(reader io.Reader, ch chan<- *ParseRedis) {
	bufReader := bufio.NewReader(reader)
	state := new(readState)
	for {
		var res RedisData
		var err error
		var msg []byte
		msg, err = readLine(bufReader, state)

		if err != nil {
			// read ended, stop reading.
			if err == io.EOF {
				ch <- &ParseRedis{
					Err: err,
				}
				close(ch)
				return
			} else {
				// Protocol error
				logger.Error(err)

				ch <- &ParseRedis{
					Err: err,
				}
				state = &readState{}
			}
			continue
		}
		// parse the read messages
		// if msg is an array or a bulk string, then parse their header first.
		// if msg is a normal line, parse it directly.

		if !state.multiLine {
			// parse single line: no bulk string

			if msg[0] == '*' {

				err := parseArrayHeader(msg, state)
				if err != nil {
					logger.Error(err)
					ch <- &ParseRedis{
						Err: err,
					}
					state = &readState{}
				} else {
					if state.arrayLen == -1 {
						// null array
						ch <- &ParseRedis{
							Data: NewArrayData(nil),
						}
						state = &readState{}
					} else if state.arrayLen == 0 {
						// empty array
						ch <- &ParseRedis{
							Data: NewArrayData([]RedisData{}),
						}
						state = &readState{}
					}
				}
				continue
			}

			if msg[0] == '$' {
				err := parseBulkHeader(msg, state)
				if err != nil {
					logger.Error(err)
					ch <- &ParseRedis{
						Err: err,
					}
					state = &readState{}
				} else {
					if state.bulkLen == -1 {
						// null bulk string
						state.multiLine = false
						state.bulkLen = 0
						res = NewBulkData(nil)
						if state.inArray {
							state.arrayData.data = append(state.arrayData.data, res)
							if len(state.arrayData.data) == state.arrayLen {
								ch <- &ParseRedis{
									Data: state.arrayData,
									Err:  nil,
								}
								state = &readState{}
							}
						} else {
							ch <- &ParseRedis{
								Data: res,
							}
						}
					}
				}
				continue
			}

			res, err = parseSingleLine(msg)
		} else {
			// parse multiple lines: bulk string (binary safe)
			state.multiLine = false
			state.bulkLen = 0
			res, err = parseMultiLine(msg)
		}

		if err != nil {
			logger.Error(err)
			ch <- &ParseRedis{
				Err: err,
			}
			state = &readState{}
			continue
		}

		// Struct parsed data as an array or a single data, and put it into channel.
		if state.inArray {
			state.arrayData.data = append(state.arrayData.data, res)
			if len(state.arrayData.data) == state.arrayLen {
				ch <- &ParseRedis{
					Data: state.arrayData,
					Err:  nil,
				}
				state = &readState{}
			}
		} else {
			ch <- &ParseRedis{
				Data: res,
				Err:  err,
			}
		}

	}
}

// Read a line or bulk line end of "\r\n" from a reader.
// Return:
//
//	[]byte: read bytes.
//	error: io.EOF or Protocol error
func readLine(reader *bufio.Reader, state *readState) ([]byte, error) {
	var msg []byte
	var err error
	if state.multiLine && state.bulkLen >= 0 {
		// read bulk line, binary safety.
		msg = make([]byte, state.bulkLen+2)
		_, err = io.ReadFull(reader, msg)
		if err != nil {
			return nil, err
		}
		state.bulkLen = 0
		if msg[len(msg)-1] != '\n' || msg[len(msg)-2] != '\r' {
			return nil, errors.New(fmt.Sprintf("Protocol error. Stream message %s is invalid.", string(msg)))
		}
	} else {
		// read normal line
		msg, err = reader.ReadBytes('\n')
		if err != nil {
			return msg, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' {
			return nil, errors.New(fmt.Sprintf("Protocol error. Stream message %s is invalid.", string(msg)))
		}
	}
	return msg, nil
}

func parseSingleLine(msg []byte) (RedisData, error) {
	// discard "\r\n"
	msgType := msg[0]
	msgData := string(msg[1 : len(msg)-2])
	var res RedisData

	switch msgType {
	case '+':
		// simple string
		res = NewStringData(msgData)
	case '-':
		// error
		res = NewErrorData(msgData)
	case ':':
		//    integer
		data, err := strconv.ParseInt(msgData, 10, 64)
		if err != nil {
			logger.Error("Protocol error: " + string(msg))
			return nil, err
		}
		res = NewIntData(data)
	default:
		// plain string
		res = NewPlainData(msgData)
	}
	if res == nil {
		logger.Error("Protocol error: parseSingleLine get nil data")
		return nil, errors.New("Protocol error: " + string(msg))
	}
	return res, nil
}

func parseMultiLine(msg []byte) (RedisData, error) {
	// discard "\r\n"
	if len(msg) < 2 {
		return nil, errors.New("protocol error: invalid bulk string")
	}
	msgData := msg[:len(msg)-2]
	res := NewBulkData(msgData)
	return res, nil
}

func parseArrayHeader(msg []byte, state *readState) error {
	// *3\r\n   -> 3
	arrayLen, err := strconv.Atoi(string(msg[1 : len(msg)-2]))
	if err != nil || arrayLen < -1 {
		return errors.New("Protocol error: " + string(msg))
	}
	state.arrayLen = arrayLen
	state.inArray = true
	state.arrayData = NewArrayData([]RedisData{})
	return nil
}

func parseBulkHeader(msg []byte, state *readState) error {
	bulkLen, err := strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil || bulkLen < -1 {
		return errors.New("Protocol error: " + string(msg))
	}
	state.bulkLen = bulkLen
	state.multiLine = true
	return nil
}
