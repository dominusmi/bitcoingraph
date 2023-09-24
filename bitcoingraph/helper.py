
import datetime
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path


def to_time(numeric_string, as_date=False):
    """
    Converts UTC timestamp to string-formatted data time.

    :param int numeric_string: UTC timestamp
    """
    if as_date:
        date_format = '%Y-%m-%d'
    else:
        date_format = '%Y-%m-%d %H:%M:%S'
    time_as_date = datetime.datetime.utcfromtimestamp(int(numeric_string))
    return time_as_date.strftime(date_format)


def to_json(raw_data):
    """
    Pretty-prints JSON data

    :param str raw_data: raw JSON data
    """
    return json.dumps(raw_data, sort_keys=True,
                      indent=4, separators=(',', ': '))


def sort(output_directory, filename, args=''):
    output_directory = Path(output_directory).resolve()
    tmp_directory = output_directory.joinpath('tmp')

    if os.path.exists(tmp_directory):
        shutil.rmtree(tmp_directory)
    os.mkdir(tmp_directory)

    cpus = os.cpu_count()
    if sys.platform == 'darwin':
        s = 'LC_ALL=C gsort -T {tmp_path} -S 50% --parallel=' + str(cpus) + ' {args} {input_filename} -o {filename}'
    else:
        s = 'LC_ALL=C sort -T {tmp_path} -S 50% --parallel=' + str(cpus) + ' {args} {input_filename} -o {filename}'

    cmd = s.format(tmp_path=tmp_directory.absolute(), args=args, input_filename=output_directory.joinpath(filename), filename=output_directory.joinpath(filename+".sorted"))
    print(f"Running: \n{cmd}")
    status = subprocess.call(cmd, shell=True)
    if status == 0:
        os.replace(output_directory.joinpath(filename+".sorted"), output_directory.joinpath(filename))
    else:
        raise Exception('unable to sort file: {}'.format(filename))
