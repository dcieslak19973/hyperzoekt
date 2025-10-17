// generated
package gen278

func Func278() int {
    s := 0
    for j:=0; j<10; j++ {
        if j%2==0 { s += j } else { s -= j }
    }
    return s
}
