from typing import Protocol

class IFoo(Protocol):
    def do_it(self) -> None: ...

class Concrete:
    def do_it(self) -> None:
        print("doing")

def top_level_variable():
    return 42
