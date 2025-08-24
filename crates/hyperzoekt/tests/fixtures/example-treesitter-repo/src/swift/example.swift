// Swift example
import Foundation

struct Point {
    var x: Double
    var y: Double

    func distanceTo(_ other: Point) -> Double {
        let dx = x - other.x
        let dy = y - other.y
        return hypot(dx, dy)
    }
}

struct Pose {
    var point: Point
    var heading: Double

    func headingTo(_ other: Point) -> Double {
        return atan2(other.y - point.y, other.x - point.x)
    }
}

let a = Point(x: 0, y: 0)
let b = Point(x: 3, y: 4)
print(a.distanceTo(b))
