protocol Drawable {
    func draw()
}

class Shape {}

class Circle: Shape, Drawable {
    func draw() {}
}

// Make Shape conform via extension
extension Shape: Drawable {
    func draw() {}
}
