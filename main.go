package main

import (
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

func time32sFormat(mem memory.Allocator, rows int, start int64) {
	ib := array.NewTime32Builder(mem, &arrow.Time32Type{Unit: arrow.Second})
	defer ib.Release()

	slice := make([]arrow.Time32, rows)
	for i := int64(0); i < int64(rows); i++ {
		slice[i] = arrow.Time32(i + start)
	}
	ib.AppendValues(slice, nil)
	value := ib.NewTime32Array()
	fmt.Printf("time32s: %s\n", value.ValueStr(0))
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
	fmt.Printf("time32ms: %s\n", value.ValueStr(0))
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
	fmt.Printf("time64ns: %s\n", value.ValueStr(0))
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
	fmt.Printf("time64us: %s\n", value.ValueStr(0))
	defer value.Release()
}

func showDefaultFormats(mem memory.Allocator, rows int, start int64) {
	time32sFormat(mem, rows, start)
	time32msFormat(mem, rows, start)
	time64usFormat(mem, rows, start)
	time64nsFormat(mem, rows, start)
}

func main() {
	rows := 1
	start := int64(100)
	mem := memory.DefaultAllocator

	showDefaultFormats(mem, rows, start)
}
