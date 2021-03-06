"""
Read leveldb trace recursively
Create my own appropriate leveldb
"""

from __future__ import division
from collections import defaultdict  # , namedtuple
import struct
from datetime import datetime  # , time
import pickle as pkl

import leveldb
from lwpb.codec import MessageCodec


class passiveHandler(object):
    """
    Attributes:
    db = leveldb database handler

    """
    def __init__(self, filename='/data/users/sarthak/filtered-20130401-20130414', folder='/data/users/sarthak/node_data201304/'):
        """
        Initialize database and codec
        """
        self.filename = filename
        self.folder = folder
        self.date_start = datetime(2013, 04, 1).date()
        self.date_end = datetime(2013, 04, 15).date()
        # mapped key format: device_id, direction, port,
        # trans_proto, domain, timestamp
        # mapped value : packetsize
        #self.mapped_format = '%s,%s,%s,%s,%s,%s'
        self.mapped_format = '%s,%s,%s,%s,%s'
        self.currentNode = ''
        self.Date = ''
        self.nodes = []
        self.NODE_LIST = []

        try:
            self.db = leveldb.LevelDB(filename)     # read leveldb
            self.codec = MessageCodec(pb2file='trace.pb2', typename='passive.Trace')    # mapped to only useful metrics
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
        dbmapped = leveldb.LevelDB(self.filename + '_' + node_id)
        self.nodes.append(node_id)

        # DATA
        self.bytespersecond = defaultdict(int)      # bytes transfer per second per node per direction
#        self.size_dist = defaultdict(int)           # packet size distribution from trace[packets series]
#        self.bytesperminute = defaultdict(int)      # size v/s timestamp from trace[packet series]
#        self.bytesperday = defaultdict(int)

#        self.port_dist_size = defaultdict(int)
#        self.port_dist_count = defaultdict(int)
        self.bytesperportperminute = defaultdict(int)
        self.requestsperportperminute = defaultdict(int)

#        self.requestsperportpersecond = defaultdict(int)

        # DEVICE v/s connected or not
        self.devices = defaultdict(list)

        self.routerActive = []

        return dbmapped

    def iterTrace(self):

        decode_it = True
        for key, value in self.db.RangeIter():
            #get key
            node_id, anon_context, session_id, sequence_number = self.parse_key(key)
            self.NODE_LIST.append(node_id)
            if node_id != self.currentNode:
                if self.currentNode != '':
                    # decode trace only if date in range, else wait till next node_id
                    decode_it = True
                    # dump old data
                    self.dumpData()
                # Reinitialize
                dbmapped = self.initialization(node_id)
                print "START", node_id

            if decode_it:
                trace = self.codec.decode(value)
                # table timestamp 30 sec granularity - after write
                currentTime = datetime.fromtimestamp(trace['trace_creation_timestamp'])
                currentDate = currentTime.date()
                if currentDate >= self.date_end:
                    decode_it = False
                    continue

                if self.Date != currentDate:
                    print currentDate
                    self.Date = currentDate

                # router activity periods
                self.routerActive.append(currentTime)

                # maintain current address table MAC:IP
                self.addressTableMaker(trace)

                # maintain current flowtable FLOWID: SRCIP, SRCPORT, DSTIP, DSTPORT, TRANS_PROTO
                self.flowTableMaker(trace)

                # TODO make DNS lookup table
                # IP : anonynymized, domain name
                # self.DNStableMaker(trace)

                # get device_id : ip -> srcip : flowid, direction / dstip : flowid,
                # direction -> flowid: timestamp, size
                # save mapped data
                dbmapped = self.packetSeriesReader(trace, dbmapped)
            else:
                continue

        return dbmapped
        #return 0

    #trace level
    def packetSeriesReader(self, trace, dbmapped):
        """
        trace['packet_series'] = [
            {'flow_id: (int), 'size': (int),
                'time_microseconds': (long int)},
            ... ]
        """
        dbbatch = leveldb.WriteBatch()

        if not 'packet_series' in trace:
            return dbmapped

        for pse in trace['packet_series']:
            flow_id = pse['flow_id']

            timestamp = pse['timestamp_microseconds']       # unix timestamp

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
#                if srcip in self.DNStable:
#                    domain, anonymized = self.DNStable[srcip]
#                    if anonymized is True:
#                        domain = 'REST'
#                #    print domain, anonymized
#                else:
#                    domain = 'UNKNOWN'
#                #    # print "NO DNS ENTRY for", srcip
#                #    continue
            elif srcip in self.addressTable:
                deviceid = self.addressTable[srcip]
                direction = 'up'
                port = dstport      # monitor port number on the server side
#                if dstip in self.DNStable:
#                    domain, anonymized = self.DNStable[dstip]
#                    if anonymized is True:
#                        domain = 'REST'
#                #    print domain, anonymized
#                else:
#                    domain = 'UNKNOWN'
                #    # print "NO DNS ENTRY for", dstip
                #    continue
            else:
                # print "Not in addressTable", dstip, srcip
                continue
            # Disable DNS table lookup for now -- April 2013
            #mappedKey = self.mapped_format % (deviceid, direction, port, trans_proto, domain, timestamp)
            if self.Date >= self.date_start:
                mappedKey = self.mapped_format % (deviceid, direction, port, trans_proto, timestamp)
                mappedValue = str(packetSize)
                # Write batch
                dbbatch.Put(mappedKey, mappedValue)

                # size distributions
                ts = datetime.fromtimestamp(timestamp * 0.000001)
                timesec = ts.replace(microsecond=0)
                timehash = timesec.replace(second=0)
                self.size_stats(packetSize, timesec.strftime("%s"), direction)
                self.port_stats(packetSize, timehash.strftime("%s"), direction, port, deviceid)
                self.device_stats(timehash.strftime("%s"), deviceid)
                #self.requestsperportpersecond[port, direction, ts.replace(microsecond=0)] += 1

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

    def testIterTrace(self, totcount):

        cnt = 0
        decode_it = False
        for key, value in self.db.RangeIter():
            node_id, anon_context, session_id, sequence_number = self.parse_key(key)
            if node_id != self.currentNode:
                decode_it = True
                #dbmapped = self.initialization(node_id)
                self.currentNode = node_id
                print(node_id)
                cnt += 1

            if decode_it:
                trace = self.codec.decode(value)

                # table timestamp 30 sec granularity - after write
                currentTime = datetime.fromtimestamp(trace['trace_creation_timestamp'])
                currentDate = currentTime.date()
                print(currentDate)
                if currentDate >= self.date_end:
                    decode_it = False
                    continue
                if self.Date != currentDate:
                    #print currentDate
                    self.Date = currentDate

                self.addressTableMaker(trace)
                self.flowTableMaker(trace)
                #self.DNStableMaker(trace)
                dbmapped = self.packetSeriesReader(trace, dbmapped)
                if cnt == totcount:
                    break
        return dbmapped
        #return

    def size_stats(self, psize, timesec, direction):

        self.bytespersecond[timesec, direction] += psize
#        timehash = timesec.replace(second=0);

        # Size distribution overall
#        self.size_dist[size] += 1

        # Bytes per minute and per day
#        datestamp = timehash.date()
#        self.bytesperminute[timehash, direction] += size
#        self.bytesperday[datestamp, direction] += size
        return

    def port_stats(self, size, timehash, direction, port, deviceid):
        #self.port_dist_size[port, direction] += size
        #self.port_dist_count[port, direction] += 1

        self.bytesperportperminute[deviceid, port, direction, timehash] += size
        self.requestsperportperminute[deviceid, port, direction, timehash] +=1
        return

    def device_stats(self, timehash, deviceid):
        if not deviceid in self.devices[timehash]:
            self.devices[timehash].append(deviceid)


    def dumpData(self):
        # DATA
        #pkl.dump(self.size_dist, open(self.folder + self.currentNode + 'size_dist.out', 'wb'))
        #pkl.dump(self.bytesperminute, open(self.folder + self.currentNode + 'bytesperminute.out', 'wb'))
        #pkl.dump(self.bytesperday, open(self.folder + self.currentNode + 'bytesperday.out', 'wb'))
        pkl.dump(self.bytespersecond, open(self.folder + self.currentNode + 'bytespersecond.out', 'wb'))

        #pkl.dump(self.port_dist_size, open(self.folder + self.currentNode + 'port_dist_size.out', 'wb'))
        #pkl.dump(self.port_dist_count, open(self.folder + self.currentNode + 'port_dist_count.out', 'wb'))
        pkl.dump(self.bytesperportperminute, open(self.folder + self.currentNode + 'bytesperportperminute.out', 'wb'))
        pkl.dump(self.requestsperportperminute, open(self.folder + self.currentNode + 'requestsperportperminute.out', 'wb'))
        #pkl.dump(self.requestsperportpersecond, open(self.folder + self.currentNode + 'requestsperportpersecond.out', 'wb'))

        pkl.dump(self.devices, open(self.folder + self.currentNode + 'device_state.out', 'wb'))

        pkl.dump(self.routerActive, open(self.folder + self.currentNode+'active.out', 'wb'))
        return


def initialize():
    pHandle = passiveHandler()
    #pHandle.testIterTrace(25)      # number of nodes
    dbm = pHandle.iterTrace()
    pHandle.dumpData()
    pkl.dump(pHandle.NODE_LIST, open('NODE_LIST.out', 'w'))
    return pHandle, dbm
    #return pHandle
