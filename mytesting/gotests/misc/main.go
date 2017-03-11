package main

import (
	"fmt"
	//"time"
)

func passtime() (*int, bool) {
	return nil, true
}

func main() {
	if err, ok := passtime(); err != nil {
		fmt.Print("error")
	} else if ok {
		fmt.Print("fine")
	}
}
