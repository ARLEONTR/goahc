package conveyor

import "fmt"

type Conveyor interface {
	Init()
	RegisterReceiver(name string) <-chan []byte
	RegisterSender(name string) chan<- []byte
	//P2PErrorChan() <-chan error
	Close()
	Done() chan struct{}
}



// Err represents a vice error.
type CommError struct {
	Message []byte
	Name    string
	CommErr     error
}


func (e *CommError) Error() string {
	if len(e.Message) > 0 {
		return fmt.Sprintf("%s: |%s| <- `%s`", e.CommErr, e.Name, string(e.Message))
	}
	return fmt.Sprintf("%s: |%s|", e.CommErr, e.Name)
}
