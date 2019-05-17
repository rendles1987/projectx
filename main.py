from tools.controller import ProcessController


def main():
    controller = ProcessController()
    controller.do_all()


if __name__ == "__main__":

    # remote debugging stuff
    from remote_debug import do_remote_debug

    do_remote_debug()

    main()
