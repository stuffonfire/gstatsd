'''
uniques = {
	metric: {
		T-0: set(),
		T-1: set()
		T-2: set()
		#...
		T-1440: set()
	}
}

minute = lambda: int(time.time()) / 60

receive(metric, unique-key):
	uniques[metric][minute()].add(unique-key)

flush:
	for k, v in uniques.iteritems():
		output(k, sum(len(s) for s in v.itervalues()))

expire:
	expired = {k for k in uniques.iterkeys() if k < (minute() - 1440)}
	map(lambda k: uniques.pop(k, None), expired)

while True
	sleep(60)
	flush()
	expire()
'''
import collections, math, time
import redis

class Uniques(object):
    host = '72.3.189.232'
    port = 6380
    db = 1
    period = 1440
    
    def __init__(self, period=1440):
        self.rd = redis.Redis(host=self.host, port=self.port, db=self.db)

    def add(self, metric, value):
        self.rd.zadd(metric, value, int(time.time()))

    def metrics(self):
        return self.rd.keys()

    def expire(self):
        pipe = self.rd.pipeline()
        for metric in self.metrics():
            pipe.zremrangebyscore(metric, 0, int(time.time()) - self.period*60)
        pipe.execute()

    def counts(self, *args):
        pipe = self.rd.pipeline()
        for a in args:
            pipe.zcard(a)
        return pipe.execute()

    def count(self, metric):
        return self.rd.zcard(metric)

    def __repr__(self):
        return '<%s[0x%x] host=%s port=%d db=%d>' % (self.__class__.__name__, id(self), self.host, self.port, self.db)

