import argparse
import time
import logging
import sys
import os

from py_async.streams.input import ICsv
from py_async.streams.output import OCsv

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


def loopback(input_fn, output_fn, input_format, output_format, start=None, limit=None):
    """
    Create input and output streams and a passthrough runner.
    """
    return None


def loopback_cmd():
    description = 'Run a loopback test which will read in a file and immediately write it back out.'

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-i', '--input', type=str, required=True)
    parser.add_argument('-o', '--output', type=str, required=True)
    parser.add_argument('-d', '--dir', type=str, default=os.getcwd())
    parser.add_argument('-l', '--limit', type=int, default=-1)
    parser.add_argument('-s', '--start', type=int, default=-1)
    parser.add_argument('-if', '--input-format', type=str, default="parquet")
    parser.add_argument('-of', '--output-format', type=str, default="parquet")

    args = parser.parse_args()

    log.debug(f'Running loopback test with input {args.input} and output {args.output}')
    start = args.start if args.start > 0 else None
    limit = args.limit if args.limit > 0 else None

    start_time = time.time()
    loopback(args.input, args.output, args.input_format, args.output_format, start, limit)
    log.debug(f'Total time : {time.time() - start_time}')


if __name__ == '__main__':
    loopback_cmd()
