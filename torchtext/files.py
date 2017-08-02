import gzip
import bz2
import tarfile
import zipfile
from os import path, environ, makedirs
import logging
import requests


class File:

    known_extensions = {'.zip', '.tar', '.bz2'}

    @staticmethod
    def download_file(url: str, local_filename: str):
        """

        Args:
            url: url to download from.
            local_filename: local file to download to.

        Returns:
            str: file name of the downloaded file.

        """
        # NOTE the stream=True parameter
        r = requests.get(url, stream=True)
        if not path.isdir(path.dirname(local_filename)):
            makedirs(path.dirname(local_filename))
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
        return local_filename

    @staticmethod
    def path(name: str):
        root = environ.get('TEXT_HOME', path.join(environ['HOME'], '.text'))
        fname = path.join(root, name)
        return fname

    @staticmethod
    def num_lines(fname):
        with File.open(fname) as f:
            return sum(1 for _ in f)

    @staticmethod
    def ensure_file(name: str, url: str=None, force: bool=False, logger: logging.Logger=logging.getLogger(), postprocess=None):
        """

        Args:
            name: name of the file, located in the `TEXT_HOME` folder.
            url: url to download the file from, if it doesn't exist.
            force: whether to force the download, regardless of the existence of the file.
            logger: logger to log results.
            postprocess: a function that, if given, will be applied after the file is downloaded. The function has the signature `f(fname)`

        Returns:
            str: file name of the downloaded file.

        """
        fname = File.path(name)
        if not path.isfile(fname) or force:
            if url:
                logger.critical('Downloading from {} to {}'.format(url, fname))
                File.download_file(url, fname)
                if postprocess:
                    postprocess(fname)
            else:
                raise Exception('{} does not exist!'.format(fname))
        return fname

    @staticmethod
    def open(fname, mode='r'):
        """
        Generic file opener

        Args:
            fname: name of the file to open.
            mode: mode for opening the file.

        Returns:
            :obj:`file`: opened file.

        """
        _, ext = path.splitext(fname)
        if fname.endswith('.tar.bz2') or fname.endswith('.tar.gz'):
            ext = '.tar'
        if ext == '.zip':
            f = zipfile.ZipFile(fname, mode=mode)
        elif ext == '.bz2':
            f = bz2.BZ2File(fname, mode=mode)
        elif ext == '.tar':
            f = tarfile.open(fname, mode=mode)
        elif ext == '.gz':
            f = gzip.open(fname, mode=mode)
        else:
            f = open(fname, mode=mode)
        return f
