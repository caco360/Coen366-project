import logging
import sys

def get_logger(name, logfile=None):
    logger = logging.getLogger(name)
    if logger.handlers:            # avoid duplicate handlers
        return logger
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(threadName)s | %(message)s")
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    logger.addHandler(console)
    if logfile:
        fh = logging.FileHandler(logfile, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger
