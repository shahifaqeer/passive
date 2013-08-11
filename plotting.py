from __future__ import division
import pickle as pkl
import pandas as pd
from collections import defaultdict
from datetime import datetime
import matplotlib.pylab as plt

NODE_LIST = pkl.load(open('/home/sarthak/Desktop/passive/scripts/NODE_LIST.dat','rb'))
#DEVICE STATE
for node in NODE_LIST:
    folder = '/home/sarthak/Desktop/passive/data/node_data/'
    dev = node+'device_state.out'
    try:
        dev_state = pkl.load(open(folder+dev,'rb'))
        device_state = defaultdict(int)
        for k,v in dev_state.items():
            {device_state[k,i]:1 for i in v}
        xx = zip(*device_state.keys())
        cnt = 1
        val = defaultdict(int)
        vals = []
        for dev in xx[1]:
            if not dev in val:
                val[dev] = cnt
                cnt += 1
            vals.append(val[dev])
        state_df = pd.DataFrame({'timestamp':xx[0], 'deviceid':xx[1], 'value':vals})
        state_df2 = state_df.pivot(index = 'timestamp', columns = 'deviceid', values='value')

        state_df2.plot(figsize = (15,8), legend=0, marker='|', linestyle='None')
        plt.ylim([0, max(val.values())+1])
        plt.yticks(val.values(), val.keys())
        plt.ylabel('device MAC address')
        plt.title('Data transfer by device for router '+node)
        #plt.show()
        print node
        plt.savefig('/home/sarthak/Desktop/passive/data/plots/active_devices_per_home/'+node+'.eps')
    except:
        print "NO DATA FOR "+node
