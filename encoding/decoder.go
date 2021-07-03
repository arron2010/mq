package encoding

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/pingcap/errors"
	"github.com/shopspring/decimal"

	proto2 "github.com/wj596/go-mysql-transfer/proto"
	"math"
	"strconv"
	"time"
)

const (
	MYSQL_TYPE_DECIMAL byte = iota
	MYSQL_TYPE_TINY
	MYSQL_TYPE_SHORT
	MYSQL_TYPE_LONG
	MYSQL_TYPE_FLOAT
	MYSQL_TYPE_DOUBLE
	MYSQL_TYPE_NULL
	MYSQL_TYPE_TIMESTAMP
	MYSQL_TYPE_LONGLONG
	MYSQL_TYPE_INT24
	MYSQL_TYPE_DATE
	MYSQL_TYPE_TIME
	MYSQL_TYPE_DATETIME
	MYSQL_TYPE_YEAR
	MYSQL_TYPE_NEWDATE
	MYSQL_TYPE_VARCHAR
	MYSQL_TYPE_BIT

	//mysql 5.6
	MYSQL_TYPE_TIMESTAMP2
	MYSQL_TYPE_DATETIME2
	MYSQL_TYPE_TIME2
)

const (
	MYSQL_TYPE_JSON byte = iota + 0xf5
	MYSQL_TYPE_NEWDECIMAL
	MYSQL_TYPE_ENUM
	MYSQL_TYPE_SET
	MYSQL_TYPE_TINY_BLOB
	MYSQL_TYPE_MEDIUM_BLOB
	MYSQL_TYPE_LONG_BLOB
	MYSQL_TYPE_BLOB
	MYSQL_TYPE_VAR_STRING
	MYSQL_TYPE_STRING
	MYSQL_TYPE_GEOMETRY
)

func isBitSet(bitmap []byte, i int) bool {
	return bitmap[i>>3]&(1<<(uint(i)&7)) > 0
}

func DecodeProtoRow(row *proto2.Row) ([][]interface{}, error) {
	needBitmap2 := false
	if row.NeedBitmap2 > 0 {
		needBitmap2 = true
	}
	r, err := DecodeRows(row.Val,
		uint64(row.ColumnCount), row.ColumnBitmap1, row.ColumnBitmap2,
		row.ColumnType, row.ColumnMeta, needBitmap2,
	)
	return r, err
}

func DecodeRows(data []byte,
	columnCount uint64, columnBitmap1 []byte, columnBitmap2 []byte,
	columnType []byte, columnMeta []uint32, needBitmap2 bool) ([][]interface{}, error) {
	rows := make([][]interface{}, 0, int(columnCount))
	pos := 0
	n := 0
	var err error
	var row []interface{}

	for pos < len(data) {
		n, row, err = decodeRows(data[pos:], columnCount, columnBitmap1, columnType, columnMeta,
			false, true, false, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}

		if needBitmap2 {
			n, row, err = decodeRows(data[pos:], columnCount, columnBitmap2, columnType, columnMeta,
				false, true, false, nil)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		pos += n
		rows = append(rows, row)
	}
	return rows, nil
}

func decodeRows(data []byte,
	columnCount uint64, bitmap []byte, columnType []byte, columnMeta []uint32,
	useDecimal bool,
	ignoreJSONDecodeErr bool, parseTime bool, timestampStringLocation *time.Location) (int, []interface{}, error) {
	row := make([]interface{}, columnCount)

	pos := 0

	// refer: https://github.com/alibaba/canal/blob/c3e38e50e269adafdd38a48c63a1740cde304c67/dbsync/src/main/java/com/taobao/tddl/dbsync/binlog/event/RowsLogBuffer.java#L63
	count := 0
	for i := 0; i < int(columnCount); i++ {
		if isBitSet(bitmap, i) {
			count++
		}
	}
	count = (count + 7) / 8

	nullBitmap := data[pos : pos+count]
	pos += count

	nullbitIndex := 0

	var n int
	var err error
	for i := 0; i < int(columnCount); i++ {
		if !isBitSet(bitmap, i) {
			continue
		}

		isNull := (uint32(nullBitmap[nullbitIndex/8]) >> uint32(nullbitIndex%8)) & 0x01
		nullbitIndex++

		if isNull > 0 {
			row[i] = nil
			continue
		}

		row[i], n, err = decodeValue(data[pos:], columnType[i], uint16(columnMeta[i]), useDecimal, ignoreJSONDecodeErr, parseTime, timestampStringLocation)

		if err != nil {
			return 0, nil, err
		}
		pos += n
	}

	return pos, row, nil
}

func decodeValue(data []byte, tp byte, meta uint16, useDecimal bool,
	ignoreJSONDecodeErr bool, parseTime bool, timestampStringLocation *time.Location) (v interface{}, n int, err error) {
	var length int = 0

	if tp == MYSQL_TYPE_STRING {
		if meta >= 256 {
			b0 := uint8(meta >> 8)
			b1 := uint8(meta & 0xFF)

			if b0&0x30 != 0x30 {
				length = int(uint16(b1) | (uint16((b0&0x30)^0x30) << 4))
				tp = byte(b0 | 0x30)
			} else {
				length = int(meta & 0xFF)
				tp = b0
			}
		} else {
			length = int(meta)
		}
	}

	switch tp {
	case MYSQL_TYPE_NULL:
		return nil, 0, nil
	case MYSQL_TYPE_LONG:
		n = 4
		v = ParseBinaryInt32(data)
	case MYSQL_TYPE_TINY:
		n = 1
		v = ParseBinaryInt8(data)
	case MYSQL_TYPE_SHORT:
		n = 2
		v = ParseBinaryInt16(data)
	case MYSQL_TYPE_INT24:
		n = 3
		v = ParseBinaryInt24(data)
	case MYSQL_TYPE_LONGLONG:
		n = 8
		v = ParseBinaryInt64(data)
	case MYSQL_TYPE_NEWDECIMAL:
		prec := uint8(meta >> 8)
		scale := uint8(meta & 0xFF)
		v, n, err = decodeDecimal(data, int(prec), int(scale), useDecimal)
	case MYSQL_TYPE_FLOAT:
		n = 4
		v = ParseBinaryFloat32(data)
	case MYSQL_TYPE_DOUBLE:
		n = 8
		v = ParseBinaryFloat64(data)
	case MYSQL_TYPE_BIT:
		nbits := ((meta >> 8) * 8) + (meta & 0xFF)
		n = int(nbits+7) / 8

		//use int64 for bit
		v, err = decodeBit(data, int(nbits), int(n))
	case MYSQL_TYPE_TIMESTAMP:
		n = 4
		t := binary.LittleEndian.Uint32(data)
		if t == 0 {
			v = formatZeroTime(0, 0)
		} else {
			v = parseFracTime(fracTime{
				Time:                    time.Unix(int64(t), 0),
				Dec:                     0,
				timestampStringLocation: timestampStringLocation,
			}, parseTime)
		}
	case MYSQL_TYPE_TIMESTAMP2:
		v, n, err = decodeTimestamp2(data, meta, timestampStringLocation)
		v = parseFracTime(v, parseTime)
	case MYSQL_TYPE_DATETIME:
		n = 8
		i64 := binary.LittleEndian.Uint64(data)
		if i64 == 0 {
			v = formatZeroTime(0, 0)
		} else {
			d := i64 / 1000000
			t := i64 % 1000000
			v = parseFracTime(fracTime{
				Time: time.Date(
					int(d/10000),
					time.Month((d%10000)/100),
					int(d%100),
					int(t/10000),
					int((t%10000)/100),
					int(t%100),
					0,
					time.UTC,
				),
				Dec: 0,
			}, parseTime)
		}
	case MYSQL_TYPE_DATETIME2:
		v, n, err = decodeDatetime2(data, meta)
		v = parseFracTime(v, parseTime)
	case MYSQL_TYPE_TIME:
		n = 3
		i32 := uint32(FixedLengthInt(data[0:3]))
		if i32 == 0 {
			v = "00:00:00"
		} else {
			sign := ""
			if i32 < 0 {
				sign = "-"
			}
			v = fmt.Sprintf("%s%02d:%02d:%02d", sign, i32/10000, (i32%10000)/100, i32%100)
		}
	case MYSQL_TYPE_TIME2:
		v, n, err = decodeTime2(data, meta)
	case MYSQL_TYPE_DATE:
		n = 3
		i32 := uint32(FixedLengthInt(data[0:3]))
		if i32 == 0 {
			v = "0000-00-00"
		} else {
			v = fmt.Sprintf("%04d-%02d-%02d", i32/(16*32), i32/32%16, i32%32)
		}

	case MYSQL_TYPE_YEAR:
		n = 1
		year := int(data[0])
		if year == 0 {
			v = year
		} else {
			v = year + 1900
		}
	case MYSQL_TYPE_ENUM:
		l := meta & 0xFF
		switch l {
		case 1:
			v = int64(data[0])
			n = 1
		case 2:
			v = int64(binary.LittleEndian.Uint16(data))
			n = 2
		default:
			err = fmt.Errorf("Unknown ENUM packlen=%d", l)
		}
	case MYSQL_TYPE_SET:
		n = int(meta & 0xFF)
		nbits := n * 8

		v, err = littleDecodeBit(data, nbits, n)
	case MYSQL_TYPE_BLOB:
		v, n, err = decodeBlob(data, meta)
	case MYSQL_TYPE_VARCHAR,
		MYSQL_TYPE_VAR_STRING:
		length = int(meta)
		v, n = decodeString(data, length)
	case MYSQL_TYPE_STRING:
		v, n = decodeString(data, length)
	case MYSQL_TYPE_JSON:
		// Refer: https://github.com/shyiko/mysql-binlog-connector-java/blob/master/src/main/java/com/github/shyiko/mysql/binlog/event/deserialization/AbstractRowsEventDataDeserializer.java#L404
		length = int(FixedLengthInt(data[0:meta]))
		n = length + int(meta)
		v, err = decodeJsonBinary(data[meta:n], useDecimal, ignoreJSONDecodeErr)
	case MYSQL_TYPE_GEOMETRY:
		// MySQL saves Geometry as Blob in binlog
		// Seem that the binary format is SRID (4 bytes) + WKB, outer can use
		// MySQL GeoFromWKB or others to create the geometry data.
		// Refer https://dev.mysql.com/doc/refman/5.7/en/gis-wkb-functions.html
		// I also find some go libs to handle WKB if possible
		// see https://github.com/twpayne/go-geom or https://github.com/paulmach/go.geo
		v, n, err = decodeBlob(data, meta)
	default:
		err = fmt.Errorf("unsupport type %d in binlog and don't know how to handle", tp)
	}
	return
}

func littleDecodeBit(data []byte, nbits int, length int) (value int64, err error) {
	if nbits > 1 {
		switch length {
		case 1:
			value = int64(data[0])
		case 2:
			value = int64(binary.LittleEndian.Uint16(data))
		case 3:
			value = int64(FixedLengthInt(data[0:3]))
		case 4:
			value = int64(binary.LittleEndian.Uint32(data))
		case 5:
			value = int64(FixedLengthInt(data[0:5]))
		case 6:
			value = int64(FixedLengthInt(data[0:6]))
		case 7:
			value = int64(FixedLengthInt(data[0:7]))
		case 8:
			value = int64(binary.LittleEndian.Uint64(data))
		default:
			err = fmt.Errorf("invalid bit length %d", length)
		}
	} else {
		if length != 1 {
			err = fmt.Errorf("invalid bit length %d", length)
		} else {
			value = int64(data[0])
		}
	}
	return
}

func decodeString(data []byte, length int) (v string, n int) {
	if length < 256 {
		length = int(data[0])

		n = int(length) + 1
		v = String(data[1:n])
	} else {
		length = int(binary.LittleEndian.Uint16(data[0:]))
		n = length + 2
		v = String(data[2:n])
	}

	return
}

const TIMEF_OFS int64 = 0x800000000000
const TIMEF_INT_OFS int64 = 0x800000

func decodeTime2(data []byte, dec uint16) (string, int, error) {
	//time  binary length
	n := int(3 + (dec+1)/2)

	tmp := int64(0)
	intPart := int64(0)
	frac := int64(0)
	switch dec {
	case 1:
	case 2:
		intPart = int64(BFixedLengthInt(data[0:3])) - TIMEF_INT_OFS
		frac = int64(data[3])
		if intPart < 0 && frac > 0 {
			/*
			   Negative values are stored with reverse fractional part order,
			   for binary sort compatibility.

			     Disk value  intpart frac   Time value   Memory value
			     800000.00    0      0      00:00:00.00  0000000000.000000
			     7FFFFF.FF   -1      255   -00:00:00.01  FFFFFFFFFF.FFD8F0
			     7FFFFF.9D   -1      99    -00:00:00.99  FFFFFFFFFF.F0E4D0
			     7FFFFF.00   -1      0     -00:00:01.00  FFFFFFFFFF.000000
			     7FFFFE.FF   -1      255   -00:00:01.01  FFFFFFFFFE.FFD8F0
			     7FFFFE.F6   -2      246   -00:00:01.10  FFFFFFFFFE.FE7960

			     Formula to convert fractional part from disk format
			     (now stored in "frac" variable) to absolute value: "0x100 - frac".
			     To reconstruct in-memory value, we shift
			     to the next integer value and then substruct fractional part.
			*/
			intPart++     /* Shift to the next integer value */
			frac -= 0x100 /* -(0x100 - frac) */
		}
		tmp = intPart<<24 + frac*10000
	case 3:
	case 4:
		intPart = int64(BFixedLengthInt(data[0:3])) - TIMEF_INT_OFS
		frac = int64(binary.BigEndian.Uint16(data[3:5]))
		if intPart < 0 && frac > 0 {
			/*
			   Fix reverse fractional part order: "0x10000 - frac".
			   See comments for FSP=1 and FSP=2 above.
			*/
			intPart++       /* Shift to the next integer value */
			frac -= 0x10000 /* -(0x10000-frac) */
		}
		tmp = intPart<<24 + frac*100

	case 5:
	case 6:
		tmp = int64(BFixedLengthInt(data[0:6])) - TIMEF_OFS
	default:
		intPart = int64(BFixedLengthInt(data[0:3])) - TIMEF_INT_OFS
		tmp = intPart << 24
	}

	if intPart == 0 {
		return "00:00:00", n, nil
	}

	hms := int64(0)
	sign := ""
	if tmp < 0 {
		tmp = -tmp
		sign = "-"
	}

	hms = tmp >> 24

	hour := (hms >> 12) % (1 << 10) /* 10 bits starting at 12th */
	minute := (hms >> 6) % (1 << 6) /* 6 bits starting at 6th   */
	second := hms % (1 << 6)        /* 6 bits starting at 0th   */
	secPart := tmp % (1 << 24)

	if secPart != 0 {
		return fmt.Sprintf("%s%02d:%02d:%02d.%06d", sign, hour, minute, second, secPart), n, nil
	}

	return fmt.Sprintf("%s%02d:%02d:%02d", sign, hour, minute, second), n, nil
}

func decodeTimestamp2(data []byte, dec uint16, timestampStringLocation *time.Location) (interface{}, int, error) {
	//get timestamp binary length
	n := int(4 + (dec+1)/2)
	sec := int64(binary.BigEndian.Uint32(data[0:4]))
	usec := int64(0)
	switch dec {
	case 1, 2:
		usec = int64(data[4]) * 10000
	case 3, 4:
		usec = int64(binary.BigEndian.Uint16(data[4:])) * 100
	case 5, 6:
		usec = int64(BFixedLengthInt(data[4:7]))
	}

	if sec == 0 {
		return formatZeroTime(int(usec), int(dec)), n, nil
	}

	return fracTime{
		Time:                    time.Unix(sec, usec*1000),
		Dec:                     int(dec),
		timestampStringLocation: timestampStringLocation,
	}, n, nil
}

func decodeJsonBinary(data []byte, useDecimal bool, ignoreJSONDecodeErr bool) ([]byte, error) {
	// Sometimes, we can insert a NULL JSON even we set the JSON field as NOT NULL.
	// If we meet this case, we can return an empty slice.
	if len(data) == 0 {
		return []byte{}, nil
	}
	d := jsonBinaryDecoder{
		useDecimal:      useDecimal,
		ignoreDecodeErr: ignoreJSONDecodeErr,
	}

	if d.isDataShort(data, 1) {
		return nil, d.err
	}

	v := d.decodeValue(data[0], data[1:])
	if d.err != nil {
		return nil, d.err
	}

	return json.Marshal(v)
}

const digitsPerInteger int = 9

var compressedBytes = []int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}

func decodeDecimal(data []byte, precision int, decimals int, useDecimal bool) (interface{}, int, error) {
	//see python mysql replication and https://github.com/jeremycole/mysql_binlog
	integral := (precision - decimals)
	uncompIntegral := int(integral / digitsPerInteger)
	uncompFractional := int(decimals / digitsPerInteger)
	compIntegral := integral - (uncompIntegral * digitsPerInteger)
	compFractional := decimals - (uncompFractional * digitsPerInteger)

	binSize := uncompIntegral*4 + compressedBytes[compIntegral] +
		uncompFractional*4 + compressedBytes[compFractional]

	buf := make([]byte, binSize)
	copy(buf, data[:binSize])

	//must copy the data for later change
	data = buf

	// Support negative
	// The sign is encoded in the high bit of the the byte
	// But this bit can also be used in the value
	value := uint32(data[0])
	var res bytes.Buffer
	var mask uint32 = 0
	if value&0x80 == 0 {
		mask = uint32((1 << 32) - 1)
		res.WriteString("-")
	}

	//clear sign
	data[0] ^= 0x80

	pos, value := decodeDecimalDecompressValue(compIntegral, data, uint8(mask))
	res.WriteString(fmt.Sprintf("%d", value))

	for i := 0; i < uncompIntegral; i++ {
		value = binary.BigEndian.Uint32(data[pos:]) ^ mask
		pos += 4
		res.WriteString(fmt.Sprintf("%09d", value))
	}

	res.WriteString(".")

	for i := 0; i < uncompFractional; i++ {
		value = binary.BigEndian.Uint32(data[pos:]) ^ mask
		pos += 4
		res.WriteString(fmt.Sprintf("%09d", value))
	}

	if size, value := decodeDecimalDecompressValue(compFractional, data[pos:], uint8(mask)); size > 0 {
		res.WriteString(fmt.Sprintf("%0*d", compFractional, value))
		pos += size
	}

	if useDecimal {
		f, err := decimal.NewFromString(String(res.Bytes()))
		return f, pos, err
	}

	f, err := strconv.ParseFloat(String(res.Bytes()), 64)
	return f, pos, err
}

func decodeDecimalDecompressValue(compIndx int, data []byte, mask uint8) (size int, value uint32) {
	size = compressedBytes[compIndx]
	databuff := make([]byte, size)
	for i := 0; i < size; i++ {
		databuff[i] = data[i] ^ mask
	}
	value = uint32(BFixedLengthInt(databuff))
	return
}

func decodeBit(data []byte, nbits int, length int) (value int64, err error) {
	if nbits > 1 {
		switch length {
		case 1:
			value = int64(data[0])
		case 2:
			value = int64(binary.BigEndian.Uint16(data))
		case 3:
			value = int64(BFixedLengthInt(data[0:3]))
		case 4:
			value = int64(binary.BigEndian.Uint32(data))
		case 5:
			value = int64(BFixedLengthInt(data[0:5]))
		case 6:
			value = int64(BFixedLengthInt(data[0:6]))
		case 7:
			value = int64(BFixedLengthInt(data[0:7]))
		case 8:
			value = int64(binary.BigEndian.Uint64(data))
		default:
			err = fmt.Errorf("invalid bit length %d", length)
		}
	} else {
		if length != 1 {
			err = fmt.Errorf("invalid bit length %d", length)
		} else {
			value = int64(data[0])
		}
	}
	return
}

const DATETIMEF_INT_OFS int64 = 0x8000000000

func decodeDatetime2(data []byte, dec uint16) (interface{}, int, error) {
	//get datetime binary length
	n := int(5 + (dec+1)/2)

	intPart := int64(BFixedLengthInt(data[0:5])) - DATETIMEF_INT_OFS
	var frac int64 = 0

	switch dec {
	case 1, 2:
		frac = int64(data[5]) * 10000
	case 3, 4:
		frac = int64(binary.BigEndian.Uint16(data[5:7])) * 100
	case 5, 6:
		frac = int64(BFixedLengthInt(data[5:8]))
	}

	if intPart == 0 {
		return formatZeroTime(int(frac), int(dec)), n, nil
	}

	tmp := intPart<<24 + frac
	//handle sign???
	if tmp < 0 {
		tmp = -tmp
	}

	// var secPart int64 = tmp % (1 << 24)
	ymdhms := tmp >> 24

	ymd := ymdhms >> 17
	ym := ymd >> 5
	hms := ymdhms % (1 << 17)

	day := int(ymd % (1 << 5))
	month := int(ym % 13)
	year := int(ym / 13)

	second := int(hms % (1 << 6))
	minute := int((hms >> 6) % (1 << 6))
	hour := int((hms >> 12))

	// DATETIME encoding for nonfractional part after MySQL 5.6.4
	// https://dev.mysql.com/doc/internals/en/date-and-time-data-type-representation.html
	// integer value for 1970-01-01 00:00:00 is
	// year*13+month = 25611 = 0b110010000001011
	// day = 1 = 0b00001
	// hour = 0 = 0b00000
	// minute = 0 = 0b000000
	// second = 0 = 0b000000
	// integer value = 0b1100100000010110000100000000000000000 = 107420450816
	if intPart < 107420450816 {
		return formatBeforeUnixZeroTime(year, month, day, hour, minute, second, int(frac), int(dec)), n, nil
	}

	return fracTime{
		Time: time.Date(year, time.Month(month), day, hour, minute, second, int(frac*1000), time.UTC),
		Dec:  int(dec),
	}, n, nil
}

func decodeBlob(data []byte, meta uint16) (v []byte, n int, err error) {
	var length int
	switch meta {
	case 1:
		length = int(data[0])
		v = data[1 : 1+length]
		n = length + 1
	case 2:
		length = int(binary.LittleEndian.Uint16(data))
		v = data[2 : 2+length]
		n = length + 2
	case 3:
		length = int(FixedLengthInt(data[0:3]))
		v = data[3 : 3+length]
		n = length + 3
	case 4:
		length = int(binary.LittleEndian.Uint32(data))
		v = data[4 : 4+length]
		n = length + 4
	default:
		err = fmt.Errorf("invalid blob packlen = %d", meta)
	}

	return
}

func parseFracTime(t interface{}, parseTime bool) interface{} {
	v, ok := t.(fracTime)
	if !ok {
		return t
	}

	if !parseTime {
		// Don't parse time, return string directly
		return v.String()
	}

	// return Golang time directly
	return v.Time
}

func FixedLengthInt(buf []byte) uint64 {
	var num uint64 = 0
	for i, b := range buf {
		num |= uint64(b) << (uint(i) * 8)
	}
	return num
}

func ParseBinaryInt8(data []byte) int8 {
	return int8(data[0])
}
func ParseBinaryUint8(data []byte) uint8 {
	return data[0]
}

func ParseBinaryInt16(data []byte) int16 {
	return int16(binary.LittleEndian.Uint16(data))
}
func ParseBinaryUint16(data []byte) uint16 {
	return binary.LittleEndian.Uint16(data)
}

func ParseBinaryInt24(data []byte) int32 {
	u32 := uint32(ParseBinaryUint24(data))
	if u32&0x00800000 != 0 {
		u32 |= 0xFF000000
	}
	return int32(u32)
}
func ParseBinaryUint24(data []byte) uint32 {
	return uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16
}

func ParseBinaryInt32(data []byte) int32 {
	return int32(binary.LittleEndian.Uint32(data))
}
func ParseBinaryUint32(data []byte) uint32 {
	return binary.LittleEndian.Uint32(data)
}

func ParseBinaryInt64(data []byte) int64 {
	return int64(binary.LittleEndian.Uint64(data))
}
func ParseBinaryUint64(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data)
}

func ParseBinaryFloat32(data []byte) float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(data))
}

func ParseBinaryFloat64(data []byte) float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(data))
}

func BFixedLengthInt(buf []byte) uint64 {
	var num uint64 = 0
	for i, b := range buf {
		num |= uint64(b) << (uint(len(buf)-i-1) * 8)
	}
	return num
}
