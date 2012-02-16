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

class Uniques(object):
    def __init__(self, period=1440):
    	self.period = period # in minutes
        self.metrics = {}

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
        return len(self.uniques(metric))

    def __repr__(self):
        return '<%s[0x%x] %s>' % (self.__class__.__name__, id(self), ' '.join('%s=%s' % (k, v) for k, v in self.__dict__.iteritems()))

