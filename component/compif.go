package component


type ComponentInterface interface {
	Init(name string)
	Connect(to Component)
	Run()
	Stop()
	ToString() string
}

