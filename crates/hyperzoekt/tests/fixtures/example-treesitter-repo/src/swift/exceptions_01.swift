import Foundation
enum MyErr: Error { case boom }
func mayThrow(_ b: Bool) throws {
    if b { throw MyErr.boom }
}
do { try mayThrow(true) } catch { print("caught") }
import Foundation
enum ExampleError: Error { case boom }
func mayThrow(_ b: Bool) throws {
    if b { throw ExampleError.boom }
}
do {
    try mayThrow(true)
} catch {
    print("caught")
}
