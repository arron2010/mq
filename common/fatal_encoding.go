package common

import "bytes"

func EncodeFatalData(mh *MarshalHelper, timestamp int64, payload []byte, errorMsg string) []byte {
	var buf bytes.Buffer
	data := []byte(errorMsg)
	mh.WriteNumber(&buf, timestamp)
	mh.WriteSlice(&buf, payload)
	mh.WriteSlice(&buf, data)
	return buf.Bytes()
}

func DecodeFatalData(mh *MarshalHelper, val []byte) (timestamp int64, payload []byte, errorMsg string) {
	buf := bytes.NewBuffer(val)
	var data []byte
	mh.ReadNumber(buf, &timestamp)
	mh.ReadSlice(buf, &payload)
	mh.ReadSlice(buf, &data)
	errorMsg = string(data)
	return timestamp, payload, errorMsg
}
