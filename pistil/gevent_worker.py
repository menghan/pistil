import os
import logging

import gevent
from pistil.worker import Worker

log = logging.getLogger(__name__)


class GreenletsArbiterWorker(Worker):

    def __str__(self):
        return 'GreenletsArbiterWorker [%d] %s' % (self.pid, self.name)

    def on_init_process(self):
        Worker.on_init_process(self)
        num_greenlets = self.conf.get('num_greenlets', 1)
        greenlet_class = self.conf.get('greenlet_class', gevent.Greenlet)
        self.greenlets = [greenlet_class(**self.conf) for i in xrange(num_greenlets)]

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


def stop_greenlet(greenlet, timeout):
    greenlet.alive = False
    greenlet.join(timeout)
    if not greenlet.ready():
        greenlet.kill()
