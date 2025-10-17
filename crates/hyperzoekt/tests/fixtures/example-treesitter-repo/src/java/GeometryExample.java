// Java example
public class GeometryExample {
    public static class Point {
        public double x, y;
        public Point(double x, double y) { this.x = x; this.y = y; }
        public double distanceTo(Point other) { double dx = x - other.x; double dy = y - other.y; return Math.hypot(dx, dy); }
    }

    public static class Pose {
        public Point p;
        public double heading;
        public Pose(Point p, double heading) { this.p = p; this.heading = heading; }
        public double headingTo(Point other) { return Math.atan2(other.y - p.y, other.x - p.x); }
    }

    public static void main(String[] args) {
        Point a = new Point(0, 0);
        Point b = new Point(3, 4);
        System.out.println(a.distanceTo(b));
    }
}
