import logging

#
# # 1 logger used thoughout whole application
# log = logging.getLogger(__name__)
# log.addHandler(logging.StreamHandler())
# log.setLevel(logging.INFO)

"""
DEBUG	    Detailed information, typically of interest only when diagnosing problems.
INFO	    Confirmation that things are working as expected.
WARNING	    An indication that something unexpected happened, or indicative of some
            problem in the near future (e.g. ‘disk space low’). The software is still
            working as expected.
ERROR	    Due to a more serious problem, the software has not been able to perform
            some function.
CRITICAL	A serious error, indicating that the program itself may be unable to
            continue running.
"""


PYTHON_FORMAT = "%(name)s %(levelname)s %(message)s"
logger = logging.getLogger(__name__)


def setup_logging():
    root_logger = logging.getLogger("")
    # Python's default log level is WARN, but we also want to see DEBUG messages.
    root_logger.setLevel(logging.DEBUG)
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    stream_handler.setFormatter(logging.Formatter(PYTHON_FORMAT))
    root_logger.addHandler(stream_handler)
