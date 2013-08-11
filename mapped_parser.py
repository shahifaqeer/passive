from __future__ import division
from collections import defaultdict  # , namedtuple
import struct
from datetime import datetime  # , time
import pickle as pkl
import leveldb
import csv
import numpy as np

NODE_LIST = pkl.load(open('NODE_LIST.dat', 'rb'))
node = 'OW2CB05D873788'

def parseMapped(node):
    folder = '/data/users/sarthak/'
    filename = 'filtered-20121001-20121101_' + node
    
    dbmapped = leveldb.LevelDB(folder+filename)
    countportdevice = defaultdict(int)
    sizeportdevice = defaultdict(int)

    #csvfile = open(folder+'testdata.csv', 'wb')
    #csvwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    for key, value in dbmapped.RangeIter():
        deviceid, direction, port, trans_proto, domain, timestamp = key.split(',')
        #csvwriter.writerow([deviceid, direction, port, timestamp, value])
        port = int(port)
        timestamp = int(timestamp) * 0.000001
        timehash = datetime.fromtimestamp(timestamp)
        timehash = timehash.replace(microsecond = 0, second =0)
        value = int(value)

        TYPE = {1:'HTTP/HTTPS', 2:'SSH', 3:'IMAP/POP3', 4:'GooglePlay', 5:'Apple Software Update', 6:'Gtalk'}
        if port == 80 or port == 443:
            TYPE = 1     #HTTP/HTTPS traffic
            countportdevice[timehash, deviceid, TYPE] += 1
            sizeportdevice[timehash, deviceid, TYPE] += value
        elif port == 22:
            TYPE = 2
            countportdevice[timehash, deviceid, TYPE] += 1
            sizeportdevice[timehash, deviceid, TYPE] += value
        elif port == 993 or port == 110:
            TYPE = 3
            countportdevice[timehash, deviceid, TYPE] += 1
            sizeportdevice[timehash, deviceid, TYPE] += value
        elif port == 5228:
            TYPE = 4
            countportdevice[timehash, deviceid, TYPE] += 1
            sizeportdevice[timehash, deviceid, TYPE] += value
        elif port == 8088:
            TYPE = 5
            countportdevice[timehash, deviceid, TYPE] += 1
            sizeportdevice[timehash, deviceid, TYPE] += value
        elif port == 19294 or port == 19295 or port == 19302:
            TYPE = 6
            countportdevice[timehash, deviceid, TYPE] += 1
            sizeportdevice[timehash, deviceid, TYPE] += value
        else:
            continue

    return countportdevice, sizeportdevice, TYPE

node = 'OW4C60DED0F74B'

def trueUtilization(node):
    '''
    [timehash] :bytes per second in a minute)
    '''
    folder = '/data/users/sarthak/'
    filename = 'filtered-20121001-20121101_' + node
    dbmapped = leveldb.LevelDB(folder+filename)
    bps_up = defaultdict(int)
    bps_dw = defaultdict(int)
    oldhour = 0

    for key, value in dbmapped.RangeIter():
        deviceid, direction, port, trans_proto, domain, timestamp = key.split(',')
        timestamp = int(timestamp) * 0.000001
        eventstamp = datetime.fromtimestamp(timestamp).replace(microsecond = 0)
        if eventstamp.hour != oldhour:
            print eventstamp
        oldhour = eventstamp.hour
        if direction == 'up':
            bps_up[eventstamp] += int(value)
        else:
            bps_dw[eventstamp] += int(value)

    pkl.dump(bps_up, open(folder+'bps/'+node+'_bps_up.out','wb'))
    pkl.dump(bps_dw, open(folder+'bps/'+node+'_bps_dw.out','wb'))

    return
