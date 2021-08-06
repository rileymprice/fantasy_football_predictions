import logging


def Logger(filename, logname="app.log"):
    logger = logging.getLogger(filename)
    log_format = "%(asctime)s ||| %(name)s ||| %(levelname)s ||| %(message)s"
    date_format = "%m-%d-%Y %H:%M:%S %Z"
    formatter = logging.Formatter(log_format, datefmt=date_format)
    file_handler = logging.FileHandler(f"logs/{logname}")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.setLevel("DEBUG")

    error_file_handler = logging.FileHandler(f"logs/error_{logname}")
    error_file_handler.setFormatter(log_format, datefmt=date_format)
    error_file_handler.setLevel(logging.ERROR)
    logger.addHandler(error_file_handler)
    return logger
