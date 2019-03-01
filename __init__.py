import sys


def main():
    from projectx.controller.seq_manager import ProcessController
    print("this works")
    controller = ProcessController()
    controller.do_all()


if __name__ == "__main__":
    sys.exit(main())


