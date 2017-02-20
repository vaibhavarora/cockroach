package main

import (
	"fmt"
)

type s struct{
    a []int
}

func main() {
    
    var sample s
    for i,each := range sample.a{
        fmt.Println(i,each)
    }
}
