import os
import sys
import pandas as pd
from config import get_params
from shredder.shredder import Shredder
from log.logger import logger


def _start(args):
    '''
    process contains
    get, read, parse csv and insert into the table
    :return: None
    '''
    if args.path is None:
        logger.exception("no path selected")
    else:
        logger.info("Getting files from folders")
        files = os.listdir(path=args.path)
        logger.info(files)

        logger.info("reading file ...")
        frames = [pd.read_csv((args.path + f),sep='\t') for f in files]
        input_df = pd.concat(frames)

        Shredder(input_df).run()

if __name__ == '__main__':
    args = get_params()
    _start(args)
