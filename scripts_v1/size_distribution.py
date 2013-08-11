from collections import defaultdict
import matplotlib.pyplot as plt 
import numpy

import leveldb
from lwpb.codec import MessageCodec

def size_dist(filename):
    size_distribution = defaultdict(int)
    db = leveldb.LevelDB(filename)
    codec = MessageCodec(pb2file='trace.pb2', typename='passive.Trace')
    for key, value in db.RangeIter():
        trace = codec.decode(value)
        for packet_series_entry in trace['packet_series']:
            size_distribution[packet_series_entry['size']] += 1
    points = sorted(size_distribution.items())
    xs, ys = zip(*points)
    plt.plot(xs, numpy.cumsum(ys)/sum(ys))
    plt.xlabel('Packet size')
    plt.ylabel('CDF')
    plt.title('Cumulative distribution of packet sizes')
    plt.savefig('packet_size_distribution.pdf')
    print 'Wrote plot to packet_size_distribution.pdf'

