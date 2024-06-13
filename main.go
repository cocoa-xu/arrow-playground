package main

import (
	"fmt"
	"math"
	"math/big"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/decimal128"
	"github.com/apache/arrow/go/v17/arrow/decimal256"
	"github.com/apache/arrow/go/v17/arrow/float16"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

func float16Format(mem memory.Allocator, rows int, _ int64) {
	ib := array.NewFloat16Builder(mem)
	defer ib.Release()

	slice := make([]float16.Num, rows)
	for i := int64(0); i < int64(rows); i++ {
		slice[i] = float16.New(float32(1.234))
	}
	ib.AppendValues(slice, nil)
	value := ib.NewFloat16Array()
	fmt.Printf("float16[default]: %s\n", value.ValueStr(0))
	defer value.Release()
}

func binaryFormat(mem memory.Allocator, rows int, start int64) {
	ib := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer ib.Release()

	slice := make([][]byte, rows)
	for i := int64(0); i < int64(rows); i++ {
		slice[i] = []byte(fmt.Sprintf("binary-%d", i+start))
	}
	ib.AppendValues(slice, nil)
	value := ib.NewBinaryArray()
	fmt.Printf("binary[default]: %s\n", value.ValueStr(0))
	defer value.Release()
}

func date32Format(mem memory.Allocator, rows int, start int64) {
	ib := array.NewDate32Builder(mem)
	defer ib.Release()

	slice := make([]arrow.Date32, rows)
	for i := int64(0); i < int64(rows); i++ {
		slice[i] = arrow.Date32(i + start)
	}
	ib.AppendValues(slice, nil)
	value := ib.NewDate32Array()
	fmt.Printf("date32[default]: %s\n", value.ValueStr(0))
	defer value.Release()
}

func date64Format(mem memory.Allocator, rows int, start int64) {
	ib := array.NewDate64Builder(mem)
	defer ib.Release()

	slice := make([]arrow.Date64, rows)
	for i := int64(0); i < int64(rows); i++ {
		slice[i] = arrow.Date64(i + start)
	}
	ib.AppendValues(slice, nil)
	value := ib.NewDate64Array()
	fmt.Printf("date64[default]: %s\n", value.ValueStr(0))
	defer value.Release()
}

func timestampNsFormat(mem memory.Allocator, rows int, start int64) {
	ib := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Nanosecond})
	defer ib.Release()

	slice := make([]arrow.Timestamp, rows)
	for i := int64(0); i < int64(rows); i++ {
		slice[i] = arrow.Timestamp(i + start)
	}
	ib.AppendValues(slice, nil)
	value := ib.NewTimestampArray()
	fmt.Printf("timestamp_ns[default]: %s\n", value.ValueStr(0))

	toTime, _ := value.DataType().(*arrow.TimestampType).GetToTimeFunc()
	encoded := toTime(value.Value(0)).Format(time.RFC3339)
	fmt.Printf("timestamp_ns[RFC3399]: %s\n", encoded)
	defer value.Release()
}

func time32sFormat(mem memory.Allocator, rows int, start int64) {
	ib := array.NewTime32Builder(mem, &arrow.Time32Type{Unit: arrow.Second})
	defer ib.Release()

	slice := make([]arrow.Time32, rows)
	for i := int64(0); i < int64(rows); i++ {
		slice[i] = arrow.Time32(i + start)
	}
	ib.AppendValues(slice, nil)
	value := ib.NewTime32Array()
	fmt.Printf("time32s[default]: %s\n", value.ValueStr(0))
	defer value.Release()
}

func time32msFormat(mem memory.Allocator, rows int, start int64) {
	ib := array.NewTime32Builder(mem, &arrow.Time32Type{Unit: arrow.Millisecond})
	defer ib.Release()

	slice := make([]arrow.Time32, rows)
	for i := int64(0); i < int64(rows); i++ {
		slice[i] = arrow.Time32(i + start)
	}
	ib.AppendValues(slice, nil)
	value := ib.NewTime32Array()
	fmt.Printf("time32ms[default]: %s\n", value.ValueStr(0))

	fmt.Printf("time32ms[Second]: %s\n", value.Value(0).FormattedString(arrow.Second))
	fmt.Printf("time32ms[Microsecond]: %s\n", value.Value(0).FormattedString(arrow.Millisecond))
	fmt.Printf("time32ms[Value]: %d\n", value.Value(0))
	defer value.Release()
}

func time64nsFormat(mem memory.Allocator, rows int, start int64) {
	ib := array.NewTime64Builder(mem, &arrow.Time64Type{Unit: arrow.Nanosecond})
	defer ib.Release()

	slice := make([]arrow.Time64, rows)
	for i := int64(0); i < int64(rows); i++ {
		slice[i] = arrow.Time64(i + start)
	}
	ib.AppendValues(slice, nil)
	value := ib.NewTime64Array()
	fmt.Printf("time64ns[default]: %s\n", value.ValueStr(0))
	defer value.Release()
}

func time64usFormat(mem memory.Allocator, rows int, start int64) {
	ib := array.NewTime64Builder(mem, &arrow.Time64Type{Unit: arrow.Microsecond})
	defer ib.Release()

	slice := make([]arrow.Time64, rows)
	for i := int64(0); i < int64(rows); i++ {
		slice[i] = arrow.Time64(i + start)
	}
	ib.AppendValues(slice, nil)
	value := ib.NewTime64Array()
	fmt.Printf("time64us[default]: %s\n", value.ValueStr(0))
	defer value.Release()
}

func decimal128Format(mem memory.Allocator, rows int, _ int64) {
	ib := array.NewDecimal128Builder(mem, &arrow.Decimal128Type{Precision: 37, Scale: 2})
	defer ib.Release()

	for i := 0; i < rows; i++ {
		v := new(big.Int).SetUint64(uint64(math.Pow(2, 64) - 1))
		v = v.Add(v, big.NewInt(int64(i)))
		ib.Append(decimal128.FromBigInt(v))
	}
	value := ib.NewDecimal128Array()
	fmt.Printf("decimal128[default]: %s\n", value.ValueStr(0))
	defer value.Release()
}

func decimal256Format(mem memory.Allocator, rows int, _ int64) {
	ib := array.NewDecimal256Builder(mem, &arrow.Decimal256Type{Precision: 76, Scale: 4})
	defer ib.Release()

	for i := 0; i < rows; i++ {
		v := new(big.Int).SetUint64(uint64(math.Pow(2, 64) - 1))
		v = v.Add(v, big.NewInt(int64(i)))
		ib.Append(decimal256.FromBigInt(v))
	}
	value := ib.NewDecimal256Array()
	fmt.Printf("decimal256[default]: %s\n", value.ValueStr(0))
	defer value.Release()
}

func durationNsFormat(mem memory.Allocator, rows int, start int64) {
	ib := array.NewDurationBuilder(mem, &arrow.DurationType{Unit: arrow.Nanosecond})
	defer ib.Release()

	slice := make([]arrow.Duration, rows)
	for i := int64(0); i < int64(rows); i++ {
		slice[i] = arrow.Duration(i + start)
	}
	ib.AppendValues(slice, nil)
	value := ib.NewDurationArray()
	fmt.Printf("durationNs[default]: %s\n", value.ValueStr(0))
	fmt.Printf("durationNs[Value]: %d\n", value.Value(0))
	defer value.Release()
}

func showDefaultFormats(mem memory.Allocator, rows int, start int64) {
	float16Format(mem, rows, start)
	binaryFormat(mem, rows, start)
	date32Format(mem, rows, start)
	date64Format(mem, rows, start)
	timestampNsFormat(mem, rows, 1000000)
	time32sFormat(mem, rows, start)
	time32msFormat(mem, rows, start)
	time64usFormat(mem, rows, start)
	time64nsFormat(mem, rows, start)
	decimal128Format(mem, rows, start)
	decimal256Format(mem, rows, start)
	durationNsFormat(mem, rows, start)
}

func main() {
	rows := 1
	start := int64(100)
	mem := memory.DefaultAllocator

	showDefaultFormats(mem, rows, start)
}
