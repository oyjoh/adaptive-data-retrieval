from abc import ABC, abstractmethod


class AbstractClassExample(ABC):
    @abstractmethod
    def do_something(self):
        print("Some implementation!")

    def do_shit(self):
        print("is work?")


class ConcreteClassExample(AbstractClassExample):
    def jalla(self):
        print("jalla")

    def do_something(self):
        super().do_something()


def main():
    concrete = ConcreteClassExample()
    concrete.jalla()
    concrete.do_something()
    concrete.do_shit()


if __name__ == "__main__":
    main()
