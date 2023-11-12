package main

import (
	"fmt"
	"time"

	"github.com/arleontr/goahc/component"
	"github.com/arleontr/goahc/pubsub"
)

func main() {

	mycompA := component.New()
	mycompA.Init("compA", pubsub.NSQ)

	mycompB := component.New()
	mycompB.Init("compB", pubsub.NSQ)

	greeting1 := "ONE-GOAHCGO "
	greeting2 := "TWO-GOAHCGO "
	time.Sleep(1 * time.Second)
	mycompB.Connect(mycompA.ID, component.SOUTH) //Place compB under compA that is why we connect A's SOUTH
	mycompA.Connect(mycompB.ID, component.NORTH) // connect B's north to A
	// TODO: first connect then run
	// Otherwise select will block on an empty channel list
	go mycompA.Run()
	go mycompB.Run()
	ctr := 1
	for i := 1; i <= 1000; i++ {

		greeting1 = fmt.Sprintf("ONE-GOAHCGO-%d", ctr)
		greeting2 = fmt.Sprintf("TWO-GOAHCGO-%d", ctr)
		mycompA.SendSouth([]byte(greeting1))
		mycompB.SendNorth([]byte(greeting2))
		time.Sleep(1 * time.Millisecond)
		ctr += 1

	}
	time.Sleep(2 * time.Second)
}
