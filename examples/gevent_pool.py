# -*- coding: utf-8 -
#
# This file is part of pistil released under the MIT license.
# See the NOTICE for more information.

import gevent
from gevent.lock import BoundedSemaphore
from pistil.pool import PoolArbiter
from pistil.gevent_worker import GreenletsArbiterWorker


class MyWorker(GreenletsArbiterWorker):

    def on_init_process(self):
        self.left_work = 10
        self.lock = BoundedSemaphore()
        self.conf['arbiter'] = self
        GreenletsArbiterWorker.on_init_process(self)


class MyGreenlet(gevent.Greenlet):

    def __init__(self, **kwargs):
        gevent.Greenlet.__init__(self)
        self.alive = True
        self.arbiter = kwargs['arbiter']

    def _run(self):
        arbiter = self.arbiter
        while self.alive:
            arbiter.lock.acquire()
            left_work = arbiter.left_work
            if arbiter.left_work <= 0:
                arbiter.lock.release()
                break
            arbiter.left_work -= 1
            arbiter.lock.release()
            print arbiter.pid, 'Begin sleeping 1 seconds'
            gevent.sleep(1)
            print arbiter.pid, 'End sleeping 1 seconds'

if __name__ == "__main__":
    conf = {"num_workers": 3, 'num_greenlets': 2, 'greenlet_class': MyGreenlet}
    spec = (MyWorker, 30, "worker", {}, "sleep")
    a = PoolArbiter(conf, spec)
    a.run()
