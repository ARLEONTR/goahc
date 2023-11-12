package main

import (
	"fmt"
	"time"

	"github.com/arleontr/goahc/component"
	"github.com/arleontr/goahc/pubsub"
)

func main() {

	mycompA := component.New()
	mycompA.Init("compA", pubsub.GOCH)
	go mycompA.Run()

	mycompB := component.New()
	mycompB.Init("compB", pubsub.GOCH)
	go mycompB.Run()

	greeting := "GO AHC GO "

	mycompB.Connect(mycompA.ID, component.SOUTH) //Place compB under compA that is why we connect A's SOUTH

	for i := 1; i <= 100000; i++ {
		fmt.Println("A sending to south")
		mycompA.SendSouth([]byte(greeting))
		time.Sleep(1 * time.Microsecond)

	}
}
