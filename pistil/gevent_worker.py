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
        conf = dict(self.conf)
        num_greenlets = conf.pop('num_greenlets', 1)
        greenlet_cls = conf.pop('greenlet_cls', gevent.Greenlet)
        a = conf.pop('cls_args', ())
        kw = conf.pop('cls_kwargs', {})
        self.greenlets = [greenlet_cls(*a, **kw) for i in xrange(num_greenlets)]

    def run(self):
        for greenlet in self.greenlets:
            greenlet.start()
        while self.alive:
            self.notify()
            if self.ppid != os.getppid():
                log.info("Parent changed, shutting down: %s", self)
                break
            if all(greenlet.ready() for greenlet in self.greenlets):
                log.info("All greenlets finished, shutting down: %s", self)
                break
            gevent.sleep(1.0)
        self.notify()
        for greenlet in self.greenlets:
            gevent.spawn(stop_greenlet, greenlet, self.timeout)
        gevent.joinall(self.greenlets)

        for greenlet in self.greenlets:
            greenlet.get()


def stop_greenlet(greenlet, timeout):
    greenlet.alive = False
    greenlet.join(timeout)
    if not greenlet.ready():
        greenlet.kill()
