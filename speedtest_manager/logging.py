"""
Module that provides logging utilities.
"""

import logging
from pathlib import Path
from logging import StreamHandler
from logging.handlers import RotatingFileHandler
from typing import Optional

def setup_logging( console_output: bool, logdir: Optional[Path], level: int = logging.INFO ) -> None:
    """
    Configures logging for the system.

    :param console_output: If True, logging output will be sent to the console.
    :param logdir: If not None, the path to the directory where log files should be stored. If None, logging
                   is not stored in a file.
    :param level: The logging level.
    """

    root = logging.getLogger()

    root.setLevel( level )

    formatter = logging.Formatter(
        fmt      = '[{asctime}] ({levelname}) {threadName}::{name} - {message}',
        datefmt = '%Y-%m-%d %H:%M:%S',
        style    = '{',
        validate = True
    )

    if console_output:
        handler = StreamHandler()
        handler.setFormatter( formatter )
        root.addHandler( handler )
    if logdir is not None:
        logdir.mkdir( mode = 0o770, parents = True, exist_ok = True )
        handler = RotatingFileHandler( logdir / 'speedtest.log', maxBytes = 10 * 1000 * 1000, backupCount = 5 )
        handler.setFormatter( formatter )
        root.addHandler( handler )
