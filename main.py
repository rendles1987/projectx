from tools.controller import ProcessController
from tools.logging import setup_logging


def main():
    setup_logging()
    controller = ProcessController()
    controller.run()


if __name__ == "__main__":
    from remote_debug import do_remote_debug

    do_remote_debug()

    main()
