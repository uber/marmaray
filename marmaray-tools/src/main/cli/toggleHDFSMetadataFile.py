"""
Copyright (c) 2018 Uber Technologies, Inc.
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions
of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.
"""

#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
A script to either enable or disable a HDFS metadata file.   A metadata file is disabled if the extension ".tmp" 
is appended at the end.  Enabling a metadata file will remove the same extension.  This tool will allow users
to disable metadata files and associated watermarks inside these files used to determine which partition to process.

Usage: toggleHDFSMetadataFile -f sample_file -d
       toggleHDFSMetadataFile -f sample_file.tmp -e
"""
import argparse, sys
import shutil
import logging
from os.path import basename, dirname, splitext, isfile

def main(args):
    logging.basicConfig(level=logging.INFO)
    disabled_ext = ".tmp"

    if not isfile(args.file):
        logging.error("the file does not exist")
        quit()

    if args.disable:
        if not args.file.endswith(disabled_ext):
            logging.info("Disabling %s", args.file)
            shutil.move(args.file, args.file + disabled_ext)
        else:
            logging.warning("the specified filename already ends with a .tmp extension")
    if args.enable:
        if args.file.endswith(disabled_ext):
            logging.info("Enabling %s ", args.file)
            shutil.move(args.file, dirname(args.file) + splitext(basename(args.file))[0])
        else:
            logging.error("the file must end with a .tmp extension")

if __name__ == '__main__':
    p= argparse.ArgumentParser()
    p.add_argument('-f', '--file', help='File to toggle on/off as metadata file')
    p.add_argument('-e', '--enable', help='Enable .tmp file as metadata file', action='store_true')
    p.add_argument('-d', '--disable', help='disable metadata file', action='store_true')
    args = p.parse_args()
    main(args)
