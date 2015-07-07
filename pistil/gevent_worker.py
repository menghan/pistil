import os
import sys
import logging

import gevent
from pistil.worker import Worker

log = logging.getLogger(__name__)


class GreenletWorker(Worker):

    def on_init_process(self):
        Worker.on_init_process(self)
        n = 5
        self.greenlets = [gevent.Greenlet(run=self.greenlet_run) for i in xrange(n)]

    def run(self):
        for greenlet in self.greenlets:
            greenlet.start()
        try:
            while self.alive:
                self.notify()
                if self.ppid != os.getppid():
                    log.info("Parent changed, shutting down: %s", self)
                    break
                gevent.sleep(1.0)
        except KeyboardInterrupt:
            pass
        try:
            self.notify()
            self.stop(timeout=self.timeout)
        except:
            pass

    def greenlet_stop(self, greenlet, timeout):
        greenlet.join(timeout=timeout)
        if not greenlet.dead:
            greenlet.kill()

    def stop(self, timeout):
        for greenlet in self.greenlets:
            gevent.spawn(self.greenlet_stop, greenlet, self.timeout)
        gevent.joinall(self.greenlets)

    def work(self):
        n = 3
        print self.pid, 'begin sleep %d seconds' % n
        gevent.sleep(n)
        print self.pid, 'end sleep %d seconds' % n

    def greenlet_run(self):
        while self.alive:
            try:
                self.work()
            except Exception as e:
                print >> sys.stderr, '[%s] Uncaught exception: %s' % (self, e)
