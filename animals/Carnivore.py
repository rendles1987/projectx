from ..tools.controller.seq_manager import ProcessController
from .Mammal import Mammals

a = ProcessController()

class Carnivore(Mammals):
    def food(self):
        print("food")
        pass
