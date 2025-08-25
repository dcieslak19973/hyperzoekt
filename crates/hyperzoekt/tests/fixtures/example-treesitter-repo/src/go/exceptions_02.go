package main
func f2() {
    defer func() { if recover() != nil { } }()
}
