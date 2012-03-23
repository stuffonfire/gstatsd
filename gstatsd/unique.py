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
    def __init__(self, period=1440):
    	self.period = period # in minutes
        self.metrics = {}
        self.expiration = 60
        self.countCache = {} # {metric: (stamp, count)}

    now = property(fget=lambda s: int(math.floor(time.time()) / 60))
    def add(self, metric, value):
        self.metrics.setdefault(metric, collections.defaultdict(set))
    	self.metrics[metric][self.now].add(value)

    def expire(self):
        for metric in self.metrics:
            buckets = self.metrics[metric]
            keys = buckets.keys()
            keys.sort()
            print '%s has %d buckets' % (metric, len(keys))
            for k in keys:
                if k < self.now - self.period:
                    print 'expiring %s[%s] because %d min old' % (metric, k, self.now - k)
                    buckets.pop(k)

    def uniques(self, metric):
        # note: can be expensive!
        rc = set()
        map(lambda b: rc.update(b), self.metrics.get(metric, {}).itervalues())
        return rc
    
    def count(self, metric):
        if self.countCache.get(metric):
            stamp, count = self.countCache[metric]
            if stamp < time.time() - self.expiration:
                self.countCache.pop(metric)
        if not self.countCache.get(metric):
            self.countCache[metric] = (time.time(), len(self.uniques(metric)))
        stamp, count = self.countCache[metric]
        return count

    def __repr__(self):
        return '<%s[0x%x] %s>' % (self.__class__.__name__, id(self), ' '.join('%s=%s' % (k, v) for k, v in self.__dict__.iteritems()))

class RedisUniques(Uniques):
    host = 'gordo2'
    port = 6380
    db = 1
    
    def __init__(self, period=1440):
        super(RedisUniques, self).__init__(period=period)
        self.rd = redis.Redis(host=self.host, port=self.port, db=self.db)

    def add(self, metric, value):
        self.rd.zadd(metric, value, int(time.time()))

    def expire(self):
        for metric in self.rd.keys():
            before = self.rd.zcard(metric)
            self.rd.zremrangebyscore(metric, 0, int(time.time()) - self.period*60)
            after = self.rd.zcard(metric)
            if before != after:
                print '%s %d before expiration %d after (%d removed)' % (metric, before, after, before-after)


    def count(self, metric):
        return self.rd.zcard(metric)

    def __repr__(self):
        return '<%s[0x%x] host=%s port=%d db=%d>' % (self.__class__.__name__, id(self), self.host, self.port, self.db)

