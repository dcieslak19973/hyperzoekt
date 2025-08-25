# Exception handling examples for Python
class MyError(Exception):
    pass

def may_raise(n):
    if n < 0:
        raise MyError("negative")
    if n == 0:
        raise ValueError("zero")
    return n

def caller():
    try:
        return may_raise(0)
    except MyError as e:
        return f"myerror:{e}"
    except Exception:
        return "other"

if __name__ == "__main__":
    print(caller())
