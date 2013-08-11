from __future__ import division
from collections import defaultdict  # , namedtuple
import struct
from datetime import datetime  # , time
import pickle as pkl

import leveldb
from lwpb.codec import MessageCodec


def parse_key(key):
    node_id, anonymization_context, remainder = key.split('\x00', 2)
    session_id, sequence_number = struct.unpack('>QI', remainder)
    session_id = session_id - 2 ** 63
    sequence_number = sequence_number - 2 ** 31
    return node_id, anonymization_context, session_id, sequence_number

filename = '/data/users/sarthak/filtered-20130401-20130414/'
db = leveldb.LevelDB(filename)
codec = MessageCodec(pb2file='trace.pb2', typename='passive.Trace')
num_dropped_packets = defaultdict(int)
NODE_LIST = []
date_start = datetime(2013,4,1)
date_end = datetime(2013,4,15)

for key, value in db.RangeIter():
    node_id, anon_context, session_id, sequence_number = parse_key(key)
    trace = codec.decode(value)
    currentTime = datetime.fromtimestamp(trace['trace_creation_timestamp'])
    if currentTime < date_end and currentTime >= date_start:
        if not(node_id in NODE_LIST):
            print node_id
            NODE_LIST.append(node_id)

    #if 'packet_series_dropped' in trace:
    #    if trace['packet_series_dropped'] > 0:
    #        num_dropped_packets[node_id, currentTime] = trace['packet_series_dropped']
    #        print node_id, currentTime, num_dropped_packets[node_id, currentTime]

#pkl.dump(num_dropped_packets, open('num_dropped_packets.out', 'w'))
pkl.dump(NODE_LIST, open('NODE_LIST.out', 'w'))
