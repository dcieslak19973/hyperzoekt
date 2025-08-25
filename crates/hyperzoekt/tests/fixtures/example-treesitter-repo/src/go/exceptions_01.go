package main
import "fmt"
func mayPanic() { panic("boom") }
func main() {
    defer func() { if r := recover(); r != nil { fmt.Println("recovered") } }()
    mayPanic()
}
