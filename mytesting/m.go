package main

import (
    "fmt"
    "reflect"
    "unsafe"
)

func BytesToString(b []byte) string {
    bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
    sh := reflect.StringHeader{bh.Data, bh.Len}
    return *(*string)(unsafe.Pointer(&sh))
}

func StringToBytes(s string) []byte {
    sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
    bh := reflect.SliceHeader{sh.Data, sh.Len, 0}
    return *(*[]byte)(unsafe.Pointer(&bh))
}

func main() {
    b := []byte{'b', 'y', 't', 'e'}
    s := BytesToString(b)
    fmt.Println(s)
    b = StringToBytes(s)
    fmt.Println(b)
}
