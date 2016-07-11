import os

from aq.errors import AQError


def ensure_data_dir_exists():
    data_dir = os.path.expanduser('~/.aq')
    if not os.path.exists(data_dir):
        try:
            os.mkdir(data_dir)
        except OSError as e:
            raise AQError('Cannot create data dir at "{}" because of: {}.'
                          'aq need a working dir to store the temporary tables before querying.'
                          ''.format(data_dir, e))


