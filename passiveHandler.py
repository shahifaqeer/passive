"""
Read leveldb trace recursively
"""

from __future__ import division
from collections import defaultdict  # , namedtuple
import struct
from datetime import datetime  # , time

import leveldb
from lwpb.codec import MessageCodec


class passiveHandler(object):
    """
    Attributes:
    db = leveldb database handler

    """
    def __init__(self, filename):
        """
        Initialize database and codec
        """
        self.filename = filename
        try:
            self.db = leveldb.LevelDB(filename)     # read leveldb
            self.codec = MessageCodec(pb2file='trace.pb2', typename='passive.Trace')    # mapped to only useful metrics
            # self.initialization()
            self.mapped_format = '%s,%s,%s,%s,%s,%s,%s'
            self.currentNode = ''
        except:
            print "Initialization Unsuccessful"
        return

    def initialization(self, node_id):
        self.currentNode = node_id
        self.flowTable = defaultdict(int)           # trace[flow_table_entry]
        self.DNStable = defaultdict(int)            # IP: (domain, anonymized)
        # [flow_id] : {dstip, dstport, srcip, srcport, transport_protocol}
        self.addressTable = defaultdict(int)        # trace[address_table_entry]
        # [mac] : {device ip}
        # self.TraceKey = namedtuple(
        #    'TraceKey',
        #    ['node_id', 'anonymization_context', 'session_id', 'sequence_number'])
        # self.currentTime = 0                        # current state time
        # self.TraceTime = []                         # list till current state time
        # self.deviceState = defaultdict(list)
        # mapped key format: node_id, device_id, direction, port,
        # trans_proto, domain, timestamp
        # mapped value : packetsize
        self.size_dist = defaultdict(int)           # packet size distribution from trace[packets series]
        self.bytesperminute = defaultdict(int)      # size v/s timestamp from trace[packet series]
        self.bytesperday = defaultdict(int)
        return

    def iterTrace(self):
        # map - reduced data used for plotting
        dbmapped = leveldb.LevelDB(self.filename + '_reduced')

        for key, value in self.db.RangeIter():
            #get key
            node_id, anon_context, session_id, sequence_number = self.parse_key(key)
            if node_id != self.currentNode:
                self.initialize()

            # trace = self.codec.decode(value)
            trace = self.codec.decode(value)

            print ('packet_series' in trace)

            # table timestamp 30 sec granularity - after write
            # self.currentTime = datetime.fromtimestamp(trace['trace_creation_timestamp'])
            '''
            # mintain current address table MAC:IP
            self.addressTableMaker(trace)
            # DEVICE v/s connected or not
            # self.deviceTime()    # plot deviceState

            # maintain current flowtable FLOWID: SRCIP, SRCPORT, DSTIP, DSTPORT, TRANS_PROTO
            self.flowTableMaker(trace)

            # make DNS lookup table
            # IP : anonynymized, domain name
            self.DNStableMaker(trace)

            # get device_id : ip -> srcip : flowid, direction / dstip : flowid,
            # direction -> flowid: timestamp, size
            # save mapped data
            dbmapped = self.packetSeriesReader(trace, node_id, dbmapped)
            '''
        return dbmapped

    #trace level
    def packetSeriesReader(self, trace, node_id, dbmapped):
        """
        trace['packet_series'] = [
            {'flow_id: (int), 'size': (int),
                'time_microseconds': (long int)},
            ... ]
        """
        dbbatch = leveldb.WriteBatch()

        for pse in trace['packet_series']:
            flow_id = pse['flow_id']
            # timestamp = datetime.fromtimestamp(pse['timestamp_microseconds'] * 0.000001)
            timestamp = datetime.fromtimestamp(pse['timestamp_microseconds'] * 0.000001)
            timestamp.replace(microsecond=0)
            packetSize = pse['size']
            # get corresponding IPs/ports from self.flowTable OR trace['flow_table_entry']
            if (flow_id != 2) and (flow_id != 4) and (flow_id in self.flowTable):
                dstip, dstport, srcip, srcport, trans_proto = self.flowTable[flow_id]
            else:
                continue
            # get corresponding devices/directions/ports
            if dstip in self.addressTable:
                deviceid = self.addressTable[dstip]
                direction = 'dw'
                port = srcport      # monitor port number on the server side
                if srcip in self.DNStable:
                    domain, anonymized = self.DNStable[srcip]
                    if anonymized is True:
                        domain = 'REST'
                #    print domain, anonymized
                else:
                    domain = 'UNKNOWN'
                #    # print "NO DNS ENTRY for", srcip
                #    continue
            elif srcip in self.addressTable:
                deviceid = self.addressTable[srcip]
                direction = 'up'
                port = dstport      # monitor port number on the server side
                if dstip in self.DNStable:
                    domain, anonymized = self.DNStable[dstip]
                    if anonymized is True:
                        domain = 'REST'
                #    print domain, anonymized
                else:
                    domain = 'UNKNOWN'
                #    # print "NO DNS ENTRY for", dstip
                #    continue
            else:
                # print "Not in addressTable", dstip, srcip
                continue
            mappedKey = self.mapped_format % (node_id, deviceid, direction,
                                              port, trans_proto, domain, timestamp)
            mappedValue = str(packetSize)

            #Write batch
            dbbatch.Put(mappedKey, mappedValue)

            # Size distribution overall
            self.size_dist[pse['size']] += 1
            # Bytes per minute and per day
            timestamp = datetime.fromtimestamp(pse['timestamp_microseconds'] * 0.000001)
            datestamp = timestamp.date()
            timehash = timestamp.replace(microsecond=0, second=0)
            self.bytesperminute[timehash] += pse['size']
            self.bytesperday[datestamp] += pse['size']

        dbmapped.Write(dbbatch, sync=True)

        return dbmapped

    def parse_key(self, key):
        node_id, anonymization_context, remainder = key.split('\x00', 2)
        session_id, sequence_number = struct.unpack('>QI', remainder)
        session_id = session_id - 2 ** 63
        sequence_number = sequence_number - 2 ** 31
        return node_id, anonymization_context, session_id, sequence_number

    def flowTableMaker(self, trace):
        if 'flow_table_entry' in trace:
            for fte in trace['flow_table_entry']:
                self.flowTable[fte['flow_id']] = (fte['destination_ip'],
                                                  fte['destination_port'],
                                                  fte['source_ip'],
                                                  fte['source_port'],
                                                  fte['transport_protocol'])
        return

    def addressTableMaker(self, trace):
        if 'address_table_entry' in trace:
            for ate in trace['address_table_entry']:
                self.addressTable[ate['ip_address']] = ate['mac_address']
        return

    def DNStableMaker(self, trace):
        if 'a_record' in trace:
            for arecord in trace['a_record']:
                self.DNStable[arecord['ip_address']] = (arecord['domain'], arecord['anonymized'])
        return

#    def deviceTime(self):
#        #self.TraceTime.append(self.currentTime)
#        [self.deviceState[k].append(self.currentTime) for k in self.addressTable.keys()]
#        return


def initialize():
    filename = 'filtered-20121101-20121201'
    pHandle = passiveHandler(filename)
    dbm = pHandle.iterTrace()
    return pHandle, dbm
