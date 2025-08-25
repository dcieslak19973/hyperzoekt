// JavaScript example
class Point {
    constructor(x, y) {
        this.x = x;
        this.y = y;
    }

    distanceTo(other) {
        const dx = this.x - other.x;
        const dy = this.y - other.y;
        return Math.hypot(dx, dy);
    }
}

class Pose {
    constructor(point, heading) {
        this.point = point;
        this.heading = heading;
    }

    headingTo(other) {
        return Math.atan2(other.y - this.point.y, other.x - this.point.x);
    }
}

function main() {
    const a = new Point(0, 0);
    const b = new Point(3, 4);
    console.log(a.distanceTo(b));
}

main();
