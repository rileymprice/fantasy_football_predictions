import logging


def Logger(filename, logname="app.log"):
    logger = logging.getLogger(filename)
    log_format = "%(asctime)s ||| %(name)s ||| %(levelname)s ||| %(message)s"
    formatter = logging.Formatter(log_format, datefmt="%m-%d-%Y %H:%M:%S %Z")
    file_handler = logging.FileHandler(f"logs/{logname}")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.setLevel("DEBUG")
    return logger
