package main

import (
	"fmt"
)


func main() {

   fmt.Println(example()) 
}

func example() (a int) {

    defer func() {
        a = 5
    }()

    a = 3

    return a
}
