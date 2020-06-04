import argparse

def get_params():
    parser = argparse.ArgumentParser()

    parser.add_argument('-path',help='path to the folder')

    args = parser.parse_args()

    return args
