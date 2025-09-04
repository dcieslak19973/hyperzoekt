package sample

type Outer struct{}

type Inner struct{}

func (o *Outer) MakeInner() *Inner { return &Inner{} }
