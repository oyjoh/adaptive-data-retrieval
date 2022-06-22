from abc import ABC, abstractmethod


class AbstractClassExample(ABC):
    @abstractmethod
    def do_something(self):
        print("Some implementation!")

    def do_shit(self):
        print("is work?")

    def tester(self):
        print(self.km)


class ConcreteClassExample(AbstractClassExample):
    def __init__(self, km) -> None:
        self.km = km

    def jalla(self):
        print("jalla")

    def do_something(self):
        super().do_something()


def main():
    lolo = AbstractClassExample()
    concrete = ConcreteClassExample("hohohoh")
    concrete.tester()


if __name__ == "__main__":
    main()
