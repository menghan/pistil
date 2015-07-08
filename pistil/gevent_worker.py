import os
import sys
import logging

import gevent
from pistil.worker import Worker

log = logging.getLogger(__name__)


class GreenletWorker(Worker):

    def __str__(self):
        return 'Worker[%d] %s' % (self.pid, self.name)

    def on_init_process(self):
        Worker.on_init_process(self)
        num_greenlets = self.conf.get('num_greenlets', 1)
        self.greenlets = [gevent.Greenlet(run=self.greenlet_run) for i in xrange(num_greenlets)]

    def run(self):
        for greenlet in self.greenlets:
            greenlet.start()
        try:
            while self.alive:
                self.notify()
                if self.ppid != os.getppid():
                    log.info("Parent changed, shutting down: %s", self)
                    break
                if all(greenlet.ready() for greenlet in self.greenlets):
                    log.info("All greenlets finished, shutting down: %s", self)
                    break
                gevent.sleep(1.0)
        except:
            pass
        try:
            self.notify()
            for greenlet in self.greenlets:
                gevent.spawn(stop_greenlet, greenlet, self.timeout)
            gevent.joinall(self.greenlets)
        except:
            pass

        for greenlet in self.greenlets:
            greenlet.get()

    def work(self):
        raise NotImplementedError

    def greenlet_run(self):
        while self.alive:
            try:
                self.work()
            except StopIteration:
                log.info('%s greenlet exit loop' % self)
                break
            except Exception as e:
                log.exception('%s Uncaught exception: %s' % (self, e))


def stop_greenlet(greenlet, timeout):
    greenlet.join(timeout)
    if not greenlet.ready():
        greenlet.kill()
