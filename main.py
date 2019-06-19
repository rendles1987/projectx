from tools.controller import ProcessController


def main():
    controller = ProcessController()
    controller.run()


if __name__ == "__main__":
    from remote_debug import do_remote_debug

    do_remote_debug()

    main()
