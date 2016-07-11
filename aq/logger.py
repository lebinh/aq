import logging

import sys

_logger = None


def get_logger():
    global _logger
    if _logger:
        return _logger

    formatter = logging.Formatter(fmt='%(levelname)s: %(message)s')
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)
    _logger = logging.getLogger('aq')
    _logger.addHandler(handler)
    return _logger


def initialize_logger(verbose=False, debug=False):
    level = logging.INFO if verbose else logging.WARNING
    if debug:
        level = logging.DEBUG
    get_logger().setLevel(level)
