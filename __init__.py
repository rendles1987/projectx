from tools.controller.seq_manager import ProcessController
import sys


def main():
    controller = ProcessController()
    controller.do_all()


if __name__ == "__main__":
    sys.exit(main())


# class Dog:
#     """A representation of a dog in 2D Land"""
#     def __init__(self, name):
#         self.name = name
#     def bark(self):
#         return "woof"
#
# class Person:
#     """A representation of a person in 2D Land"""
#     def __init__(self, name):
#         self.name = name
#     def make_noise(self):
#         return "hello"
#
# class DogAdapter:
#     """Adapts the Dog class through encapsulation"""
#     def __init__(self, canine):
#         self.canine = canine
#     def make_noise(self):
#         """This is the only method that's adapted"""
#         return self.canine.bark()
#     def __getattr__(self, attr):
#         """Everything else is delegated to the object"""
#         return getattr(self.canine, attr)
#
# def click_creature(creature):
#     """React to a click by showing the creature's name and what is says"""
#     return creature.name, creature.make_noise()
#
# def exercise_system():
#     person = Person("Bob")
#     canine = DogAdapter(Dog("Fido"))
#     for critter in (person, canine):
#         print(critter.name, "says", critter.make_noise())
#
# if __name__ == "__main__":
#     exercise_system()





