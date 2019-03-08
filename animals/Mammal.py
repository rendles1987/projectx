

class Mammals(object):
    extremities = 4

    def feeds(self):
        print ("milk")

    def proliferates(self):
        pass


class MarsupialAbc(Mammals):
        def proliferates(self):
            print("poach")


class Eutherian(Mammals):
        def proliferates(self):
            print("placenta")


a = MarsupialAbc()
a.proliferates()
