def deco(f):
    def wrapper(*a, **k):
        return f(*a, **k)
    return wrapper

@deco
def decorated_func():
    return "ok"

class WithDecorator:
    @deco
    def method(self):
        pass
