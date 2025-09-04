public class Person {
    public var name: String
    public init(name: String) { self.name = name }

    public func greet() {
        print("hello \(name)")
    }
}

extension Person {
    // method added via extension
    public func salute() {
        print("salute \(name)")
    }

    // static extension method
    public static func extStatic() {}
}
