#! /usr/bin/env python3

import argparse
import logging

def logger(f):
    def inner(*args, **kwargs):
        name = f'{__name__}.{f.__name__}'
        return f(*args, **kwargs, log=logging.getLogger(name))
    return inner

@logger
def main(log):
    parser = argparse.ArgumentParser()

    parser.add_argument('file', help='the file to mirror')
    parser.add_argument('--verbose', '-v', action='count', default=0,
            help='verbosity level, repeat to increase')

    args = parser.parse_args()

    logging.basicConfig(level=max(10, 30-(args.verbose * 10)))
    log.debug(f'called with arguments {vars(args)}')

if __name__ == '__main__':
    main()
