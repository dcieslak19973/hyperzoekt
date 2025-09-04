pub trait Drawable {
    fn draw(&self);
}

pub struct Circle;

impl Drawable for Circle {
    fn draw(&self) {}
}

pub struct Shape;

impl Drawable for Shape {
    fn draw(&self) {}
}
