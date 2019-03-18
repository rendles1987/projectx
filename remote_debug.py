def do_remote_debug():
    import sys
    import os
    if not (os.path.isdir('/pycharm-helpers/')):
        msg = 'debug_egg dir does not exist. Make sure you add it as volume in' \
              'docker-compose.yml \n' \
              'volumes:\n' \
              '- /<dir_debug_eggs>:/pycharm-helpers'
        raise AssertionError(msg)
    if not (os.path.exists('/pycharm-helpers/pycharm-debug-py3k.egg')):
        msg = "cant find 'pycharm-debug-py3k.egg'. pycharm-debug.egg is for " \
              "python2. This is python3"
        raise AssertionError(msg)

    sys.path[0:0] = ['/pycharm-helpers/pycharm-debug-py3k.egg', ]
    import pydevd
    # pydevd.settrace('10.90.20.40', port=4444, stdoutToServer=True,
    #             stderrToServer=True, suspend=True)
    # pydevd.settrace('10.90.20.43', port=4444, suspend=False,
    #                 stdoutToServer=True)
    pydevd.settrace('192.168.1.97', port=4444, suspend=False,
                    stdoutToServer=True)

