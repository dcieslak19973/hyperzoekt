# Python example
import math

class Point:
    def __init__(self, x, y):
        self.x = x
        self.y = y

    def distance_to(self, other):
        dx = self.x - other.x
        dy = self.y - other.y
        return math.hypot(dx, dy)

class Pose:
    def __init__(self, point, heading):
        self.point = point
        self.heading = heading

    def heading_to(self, other):
        return math.atan2(other.y - self.point.y, other.x - self.point.x)

if __name__ == "__main__":
    a = Point(0, 0)
    b = Point(3, 4)
    print(a.distance_to(b))
