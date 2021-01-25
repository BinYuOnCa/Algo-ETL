import os
import shutil
import pathlib
import fnmatch
from utils.log import init_log
from utils.log import output_log
import utils.param as param


def archive_data(src: str, dst: str, pattern: str = '*'):
    """
    moving data file from one dir to another dir
    :param src: (str)
    :param dst: (str)
    :param pattern: (str) optional
    :return
    """
    try:
        if not os.path.isdir(dst):
            pathlib.Path(dst).mkdir(parents=True, exist_ok=True)
        for f in fnmatch.filter(os.listdir(src), pattern):
            shutil.move(os.path.join(src, f), os.path.join(dst, f))
        output_log(f'move data files from {src} to {dst} successfully')
    except OSError as e:
        output_log(e, param.LOG_LEVEL_ERROR)


def cleanup_archive_data(dir_path: str):
    """
    clean up archive folder data
    :param dir_path: (str)
    :return
    """
    try:
        # delete a given directory including its content
        shutil.rmtree(dir_path)
        if not os.path.isdir(dir_path):
            # create a directory
            pathlib.Path(dir_path).mkdir(parents=True, exist_ok=True)
        output_log(f'clean up folder {dir_path} successfully')
    except OSError as e:
        output_log(e, param.LOG_LEVEL_ERROR)


if __name__ == "__main__":
    """
    clean up data in archive folder, trigger by con job
    """
    init_log()
    cleanup_archive_data(param.ARCHIVE_PATH)
