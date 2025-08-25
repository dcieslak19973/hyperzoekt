using System;
class Ex02 {
    void Run() {
        try { throw new InvalidOperationException(); } catch (Exception e) { Console.WriteLine(e); }
    }
}
