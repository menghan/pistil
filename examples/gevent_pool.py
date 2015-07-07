# -*- coding: utf-8 -
#
# This file is part of pistil released under the MIT license.
# See the NOTICE for more information.

from pistil.pool import PoolArbiter
from pistil.gevent_worker import GreenletWorker


if __name__ == "__main__":
    conf = {"num_workers": 3}
    spec = (GreenletWorker, 30, "worker", {}, "test")
    a = PoolArbiter(conf, spec)
    a.run()
