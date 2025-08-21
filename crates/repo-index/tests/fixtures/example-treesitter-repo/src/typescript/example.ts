// @ts-nocheck
// TypeScript example
export { };
class Point {
    x: number;
    y: number;

    constructor(x: number, y: number) {
        this.x = x;
        this.y = y;
    }

    distanceTo(other: Point): number {
        const dx = this.x - other.x;
        const dy = this.y - other.y;
        return Math.hypot(dx, dy);
    }
}

class Pose {
    point: Point;
    heading: number;

    constructor(point: Point, heading: number) {
        this.point = point;
        this.heading = heading;
    }

    headingTo(other: Point): number {
        return Math.atan2(other.y - this.point.y, other.x - this.point.x);
    }
}

function main() {
    const a = new Point(0, 0);
    const b = new Point(3, 4);
    console.log(a.distanceTo(b));
}

main();
