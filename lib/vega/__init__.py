import logging as log
log.basicConfig(
    level=log.INFO,
    datefmt="%Y%m%d-%H:%M:%S",
    format=f'{__name__}-%(levelname)s-%(asctime)s-%(funcName)s: %(message)s')