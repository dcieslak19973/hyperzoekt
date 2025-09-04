public enum Direction {
    case north
    case south
    case east
    case west
}

enum Result<T> {
    case ok(T)
    case error(String)
}
