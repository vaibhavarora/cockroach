package main

import (
	"fmt"
)

type lock struct {
	id int
}

func main() {

    var l *lock

    if l == nil{
        fmt.Printf("is nil")
    }

}
