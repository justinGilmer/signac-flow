# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import uuid
import time
import random
from itertools import count


__all__ = ['DocumentLock']


class DocumentLock(object):

    def __init__(self, document, key='_lock'):
        self._document = document
        self._key = key
        self._id = None

    def _acquire(self):
        if self._document.get(self._key):
            return False
        else:
            _id = str(uuid.uuid4())
            self._document[self._key] = _id
            locked = self._document[self._key] == _id
            if locked:
                self._id = _id
            return locked

    def acquire(self, timeout=-1, delay=0.1):
        start = time.time()
        for n in count(1):
            if self._acquire():
                break
            now = time.time()
            if timeout < 0:
                duration = delay * (n + random.random())
                time.sleep(duration)
            elif now <= start + timeout:
                duration = min(delay * n, start + timeout - now) + random.random() * delay
                time.sleep(duration)
            else:
                raise RuntimeError("Failed to lock.")

    def release(self, force=True):
        assert force or self._id is not None
        self._document[self._key] = None

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, etype, evalue, traceback):
        self.release()
