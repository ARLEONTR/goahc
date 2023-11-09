package main

import (
	"goahc/component"
	"time"
)


func main (){

	mycompA := component.New()
	mycompA.Init("compA")
	mycompA.Run()

	mycompB := component.New()
	mycompB.Init("compB")
	mycompB.Run()


	for (true){
		time.Sleep(1 *time.Second)
		greeting := "GO AHC GO " 
		compAconnector := mycompB.Commch.RegisterSender("ID-"+mycompA.ToString())
		compAconnector <- []byte(greeting)
	}
}