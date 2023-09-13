import stat
import pickle
from files import FileReader

class XferSender(object):
    def __init__(self, seg_len:int, reader:FileReader) -> None:
        self.seg_len:int = seg_len
        self.reader:FileReader = reader

    def data_segments(self):
        for entry in self.reader.list_files():
            meta = pickle.dumps(entry)
            meta_len = len(meta)
            meta_payload = meta_len.to_bytes(4, 'little') + meta

            yield meta_payload
            if stat.S_ISREG(entry.get('stat').st_mode):
                f = open(self.reader.file_path(entry.get('path')), 'rb')
                data = f.read(self.seg_len)
                while data:
                    yield data
                    data = f.read(self.seg_len)


class XferReceiver(object):
    def __init__(self) -> None:
        # pre_meta -> meta -> [data] -> done
        self.status = 'pre_meta'
        self.remain_bytes = 4
        self.buffer = bytes()

    def fill_buffer(self, data:bytes):
        data_remain = len(data)
        truncate_len = min(self.remain_bytes, data_remain)
        self.buffer += data[:truncate_len]
        data_remain -= truncate_len
        self.remain_bytes -= truncate_len
        return data_remain

    def receive_from_bytes(self, data:bytes):
        data_len = len(data)
        data_remain = data_len

        while data_remain > 0:
            if self.status == 'pre_meta' or self.status == 'meta':
                data_remain = self.fill_buffer(data)

            if self.status == 'pre_meta' and self.remain_bytes == 0:
                self.status = 'meta'
                self.remain_bytes = int.from_bytes(self.buffer, 'little')
                self.buffer = bytes()
                data_remain = self.fill_buffer(data[data_len - data_remain:])

            if self.status == 'meta' and self.remain_bytes == 0:
                meta = pickle.loads(self.buffer)
                mode = meta.get('stat').st_mode
                if stat.S_ISREG(mode):
                    self.status = 'data'
                    self.remain_bytes = meta.get('stat').st_size
                else:
                    self.status = 'pre_meta'
                    self.remain_bytes = 4

                self.buffer = bytes()
                yield meta
                continue

            if self.status == 'data': # do not fill buffer when xfer data
                rlen = min(self.remain_bytes, data_remain)
                start_ind = data_len - data_remain
                self.remain_bytes -= rlen
                data_remain -= rlen

                yield data[start_ind: + rlen]

                if self.remain_bytes == 0:
                    self.status = 'pre_meta'
                    self.remain_bytes = 4
                    yield bytes()

if __name__ == '__main__':
    reader = FileReader('../nvimdots')
    sender = XferSender(16384, reader)
    data_generator = sender.data_segments()
    while True:
        data = next(data_generator)
        print(data)

