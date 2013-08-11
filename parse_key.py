from collections import namedtuple
import struct

TraceKey = namedtuple(
        'TraceKey',
        ['node_id', 'anonymization_context', 'session_id', 'sequence_number'])

def parse_key(key):
    node_id, anonymization_context, remainder = key.split('\x00', 2)
    session_id, sequence_number = struct.unpack('>QI', remainder)
    return TraceKey(
            node_id=node_id,
            anonymization_context=anonymization_context,
            session_id=session_id - 2**63,
            sequence_number=sequence_number - 2**31)
            
# See https://github.com/sburnett/transformer/blob/master/key/key.go for more details
# on how I chose to encode keys.