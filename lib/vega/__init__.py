import logging as log
log.basicConfig(
    level=log.INFO,
    datefmt="%Y%m%d-%H:%M:%S",
    format=f'{__name__}-%(asctime)s-%(funcName)s-%(levelname)s: %(message)s')