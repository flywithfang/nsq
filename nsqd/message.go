package nsqd

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"math/rand"
	"time"
)

const (
	MsgIDLength         = 16
	minV1ValidMsgLength = MsgIDLength + 8 + 2         // Timestamp + Attempts
	minV2ValidMsgLength = MsgIDLength + 8 + 2 + 1 + 4 // Timestamp + Attempts+ver+routinghash
)
const (
	MSG_V2  uint8 = 0x80 //0x80
	MSG_RPC uint8 = 0x01
)

func HashString(s string) uint32 {
	var fnvHash hash.Hash32 = fnv.New32a()
	fnvHash.Write([]byte(s))
	return fnvHash.Sum32()
}

type MessageID [MsgIDLength]byte

type Message struct {
	Version     uint8
	RoutingHash uint32
	ID          MessageID
	Body        []byte
	Timestamp   int64
	Attempts    uint16
	srcClientID int64
	// for in-flight handling
	deliveryTS time.Time
	clientID   int64
	pri        int64
	index      int
	deferred   time.Duration
}

func NewMessage(id MessageID, body []byte) *Message {
	return NewMessageV2(id, body, "", 0)
}
func NewMessageV2(id MessageID, body []byte, routingKey string, srcClientID int64) *Message {
	var hashCode uint32 = 0
	if len(routingKey) == 0 {
		hashCode = rand.Uint32()
	} else {
		hashCode = HashString(routingKey)
	}
	////log.Printf("new msg id %v, routing key %v, hash %d\n", string(id[:]), routingKey, hashCode)
	version := MSG_V2
	if srcClientID > 0 {
		version = version | MSG_RPC
	}
	return &Message{
		ID:          id,
		Body:        body,
		Timestamp:   time.Now().UnixNano(),
		Version:     version,
		RoutingHash: hashCode,
		srcClientID: srcClientID,
	}
}

func (m *Message) WriteToV1(w io.Writer) (int64, error) {
	var buf [10]byte
	var total int64
	binary.BigEndian.PutUint64(buf[0:8], uint64(m.Timestamp))
	binary.BigEndian.PutUint16(buf[8:10], uint16(m.Attempts))

	n, err := w.Write(buf[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.ID[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.Body)
	total += int64(n)
	if err != nil {
		return total, err
	}

	return total, nil
}
func (m *Message) WriteTo(w io.Writer) (int64, error) {
	var buf [10]byte
	var total int64
	//Ver,Hash,Timestamp,Attemps,ID,Body
	buf[0] = m.Version
	total += 1
	binary.BigEndian.PutUint32(buf[1:5], m.RoutingHash)
	n, err := w.Write(buf[:5])
	total += int64(n)
	if err != nil {
		return total, err
	}

	binary.BigEndian.PutUint64(buf[0:8], uint64(m.Timestamp))
	binary.BigEndian.PutUint16(buf[8:10], uint16(m.Attempts))
	n, err = w.Write(buf[0:10])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.ID[:])
	total += int64(n)
	if err != nil {
		return total, err
	}
	if m.srcClientID > 0 && m.Version&MSG_RPC > 0 {
		binary.BigEndian.PutUint64(buf[:8], uint64(m.srcClientID))
		n, err = w.Write(buf[:8])
		total += int64(n)
		if err != nil {
			return total, err
		}
	}
	n, err = w.Write(m.Body)
	total += int64(n)
	if err != nil {
		return total, err
	}

	return total, nil
}

// decodeMessage deserializes data (as []byte) and creates a new Message
// message format:
// [x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x]...
// |       (int64)        ||    ||      (hex string encoded in ASCII)           || (binary)
// |       8-byte         ||    ||                 16-byte                      || N-byte
// ------------------------------------------------------------------------------------------...
//   nanosecond timestamp    ^^                   message ID                       message body
//                        (uint16)
//                         2-byte
//                        attempts
func decodeMessage(b []byte) (*Message, error) {
	var msg Message

	if len(b) < minV1ValidMsgLength {
		return nil, fmt.Errorf("invalid message buffer size (%d)", len(b))
	}
	ver := b[0]
	if ver&MSG_V2 > 0 {
		msg.Version = ver
		msg.RoutingHash = uint32(binary.BigEndian.Uint32(b[1:5]))
		msg.Timestamp = int64(binary.BigEndian.Uint64(b[5:13]))
		msg.Attempts = binary.BigEndian.Uint16(b[13:15])
		copy(msg.ID[:], b[15:15+MsgIDLength])
		b = b[15+MsgIDLength:]
		if ver&MSG_RPC > 0 {
			msg.srcClientID = int64(binary.BigEndian.Uint64(b[:8]))
			b = b[8:]
		}
		msg.Body = b[:]
	} else {
		msg.Version = MSG_V2
		msg.RoutingHash = 0 //keep old msg in one process
		msg.Timestamp = int64(binary.BigEndian.Uint64(b[:8]))
		msg.Attempts = binary.BigEndian.Uint16(b[8:10])
		copy(msg.ID[:], b[10:10+MsgIDLength])
		msg.Body = b[10+MsgIDLength:]
	}

	return &msg, nil
}

func writeMessageToBackend(buf *bytes.Buffer, msg *Message, bq BackendQueue) error {
	buf.Reset()
	_, err := msg.WriteTo(buf)
	if err != nil {
		return err
	}
	return bq.Put(buf.Bytes())
}
