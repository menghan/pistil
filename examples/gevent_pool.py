# -*- coding: utf-8 -
#
# This file is part of pistil released under the MIT license.
# See the NOTICE for more information.

import gevent
from gevent.lock import BoundedSemaphore
from pistil.pool import PoolArbiter
from pistil.gevent_worker import GreenletWorker


class MyWorker(GreenletWorker):

    def on_init_process(self):
        GreenletWorker.on_init_process(self)
        self.left_work = 10
        self.lock = BoundedSemaphore()

    def work(self):
        self.lock.acquire()
        left_work = self.left_work
        if self.left_work <= 0:
            self.lock.release()
            raise StopIteration
        self.left_work -= 1
        self.lock.release()
        print self.pid, 'Begin sleeping 1 seconds'
        gevent.sleep(1)
        print self.pid, 'End sleeping 1 seconds'

if __name__ == "__main__":
    conf = {"num_workers": 3, 'num_greenlets': 2}
    spec = (MyWorker, 30, "worker", {}, "sleep")
    a = PoolArbiter(conf, spec)
    a.run()
