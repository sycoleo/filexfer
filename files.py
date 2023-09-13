from asyncio.tasks import wait_for
import os
import stat
import time
import pickle


def WARN(msg:str):
    print(msg)

class FileReader(object):
    def __init__(self, path:str) -> None:
        self.path:str = os.path.realpath(path)
        self.dirname:str = os.path.basename(self.path)

    def get_file(self, path:str, is_subdir:bool=False):
        fstat = os.stat(path)
        relpath = os.path.relpath(path, self.path)
        target = None

        if stat.S_ISLNK(fstat.st_mode):
            target = os.readlink(path)

        elif stat.S_ISDIR(fstat.st_mode):
            if is_subdir:
                yield {
                    'path': os.path.join(self.dirname, relpath),
                    'stat': fstat,
                    'link_target': target
                    }
            else:
                yield {
                    'path': self.dirname,
                    'stat': fstat,
                    'link_target': target
                    }
            for d in os.listdir(path):
                for entry in self.get_file(os.path.join(path, d), True):
                    yield entry

        elif stat.S_ISREG(fstat.st_mode):
            yield {
                'path': os.path.join(self.dirname, relpath),
                'stat': fstat,
                'link_target': target
                }

        else:
            WARN('ignore unsupported file: %s' % path)

    def list_files(self):
        return self.get_file(self.path)

    def file_path(self, relpath):
        return os.path.join(os.path.dirname(self.path), relpath)

    def file_serialize(self):
        for entry in self.list_files():
            data = bytes()
            if stat.S_ISREG(entry.get('stat').st_mode):
                f = open(self.file_path(entry.get('path')), 'rb')
                data = f.read()
            yield {
                'meta': pickle.dumps(entry),
                'data': data
            }

class FileWriter(object):
    def __init__(self, path:str, verbose=True) -> None:
        if not os.path.exists(path):
            raise RuntimeError('Directory %s not exist' % path)

        self.path = os.path.realpath(path)
        self.fd = None
        self.reg_meta = None
        self.verbose = verbose
        self.write_start = 0

    def create(self, meta:dict) -> None:
        path = os.path.join(self.path, meta.get('path'))
        mode = meta.get('stat').st_mode
        if not stat.S_ISREG(mode):
            if stat.S_ISDIR(mode):
                os.makedirs(path, exist_ok=True)
            elif stat.S_ISLNK(mode):
                os.symlink(path, meta.get('link_target'))

            os.utime(path, times=(meta.get('stat').st_atime, meta.get('stat').st_mtime))
            os.chmod(path, stat.S_IMODE(mode))
        else:
            if self.verbose:
                print('xfer', path, end='')
            self.fd = open(path, 'wb')
            self.reg_meta = meta
            self.write_start = time.time()

    def write_reg(self, data:bytes) -> None:
        assert(self.fd != None)
        if data == bytes():
            size = self.reg_meta.get('stat').st_size
            self.fd.close()
            self.fd = None
            self.reg_meta = None
            elapsed = time.time() - self.write_start
            if self.verbose:
                print(
                    " %d bytes in %.1f s (%.3f MBps)"
                    % (size, elapsed, size / elapsed / 1000000)
                )
            return

        self.fd.write(data)

def file_writer(basedir:str, meta:bytes, data:bytes):
    if not os.path.exists(basedir):
        raise RuntimeError('Destination not exist:', basedir)

    meta_data = pickle.loads(meta)
    path = os.path.join(basedir, meta_data.get('path'))
    mode = meta_data.get('stat').st_mode
    if stat.S_ISREG(mode):
        fd = open(path, 'wb')
        fd.write(data)
        fd.close()
    elif stat.S_ISDIR(mode):
        os.makedirs(path, stat.S_IMODE(mode), exist_ok=True)
    elif stat.S_ISLNK(mode):
        os.symlink(path, meta_data.get('link_target'))

    os.utime(path, times=(meta_data.get('stat').st_atime, meta_data.get('stat').st_mtime))
    os.chmod(path, stat.S_IMODE(mode))

def get_data_size(meta:bytes) -> int:
    meta_data = pickle.loads(meta)
    mode = meta_data.get('stat').st_mode
    if stat.S_ISREG(mode):
        return meta_data.get('stat').st_size
    else:
        return 0

if __name__ == '__main__':
    f = FileReader('../nvimdots')
    for s in f.file_serialize():
        file_writer('.', s.get('meta'), s.get('data'))

