pub struct Point {
    x: f64,
    y: f64,
}

impl Point {
    pub fn new(x: f64, y: f64) -> Self {
        Point { x, y }
    }
    pub fn distance_to(&self, other: &Point) -> f64 {
        let dx = other.x - self.x;
        let dy = other.y - self.y;
        (dx * dx + dy * dy).sqrt()
    }
    pub fn heading_to(&self, other: &Point) -> f64 {
        (other.y - self.y).atan2(other.x - self.x)
    }
}

pub struct Pose {
    point: Point,
    heading: f64,
}

pub struct Animal {
    name: String,
    pose: Pose,
}

impl Animal {
    pub fn new(name: &str, pose: Pose) -> Self {
        Animal {
            name: name.to_string(),
            pose,
        }
    }
    pub fn distance_and_heading_to(&self, other: &Animal) -> (f64, f64) {
        (
            self.pose.point.distance_to(&other.pose.point),
            self.pose.heading_to(&other.pose.point),
        )
    }
}
