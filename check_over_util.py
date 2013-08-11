from __future__ import division
import pickle as pkl
import pandas as pd
from collections import defaultdict
from datetime import datetime
import matplotlib.pylab as plt
import numpy as np

folder = '/home/sarthak/Desktop/passive/check_over_util/'
outfolder = '/home/sarthak/Desktop/passive/check_over_util/'
#NODE_LIST = pkl.load(open(folder + 'NODE_LIST_FINAL.out', 'r'))
NODE_LIST = ['OW2CB05D873788', 'OW100D7F64C8A3']
home_num = pkl.load(open(outfolder + 'home_num_final.out', 'r'))


def loadActiveBitrate(node):
    #load active readings of kbps
    cap_dw = zip(*pkl.load(open(folder + 'capacity201304/' + node + '_dw.out', 'rb')))
    cap_up = zip(*pkl.load(open(folder + 'capacity201304/' + node + '_up.out', 'rb')))

    return cap_dw, cap_up


def assignHomeNumbers():

    # get active capacity using pgsql program in dp4~bismark-active/bitrate/
    active_cap_up = []
    active_cap_dw = []
    for node in NODE_LIST:
        cap_dw, cap_up = loadActiveBitrate(node)

        active_cap_dw.append(np.percentile(np.array(cap_dw[1])*128, 99))
        active_cap_up.append(np.percentile(np.array(cap_up[1])*128, 99))

    df_home_active = pd.DataFrame({'router':NODE_LIST, 'active_capacity_up': active_cap_up, 'active_capacity_dw': active_cap_dw}).sort(columns='active_capacity_dw', ascending=False)
    df_home_active['home'] = range(1, len(df_home_active)+1)

    #df_home_active.save('node_data201304/home_numbers.pda')
    home_num = df_home_active.set_index('router').home.to_dict()
    pkl.dump(home_num, open(outfolder + 'home_num.out', 'w'))

    return home_num


def loadPassiveBitrate(node):
    #load passive readings of Bps
    bps = pkl.load(open(folder + node + 'bytespersecond.out', 'rb'))
    timestamp, direction = zip(*bps)
    packetsize = bps.values()
    df_bps = pd.DataFrame({'timestamp': timestamp, 'direction': direction, 'packetsize': packetsize}).sort(columns='timestamp')
    #bytes per second in a minute
    df_bps['eventstamp'] = df_bps.timestamp.apply(lambda x: datetime.fromtimestamp(int(x)))

    #max Bps represents reading for that minute
    #comment below line if don't want max reading to represent a minute
    df_bps['eventstamp'] = df_bps['eventstamp'].apply(lambda x: x.replace(second=0))
    df_bps_unique = df_bps.set_index(['eventstamp', 'direction']).groupby(level=[0,1]).max()
    temp_df = df_bps_unique.reset_index()

    passive_bitrate_up = temp_df[temp_df.direction == 'up'][['eventstamp','packetsize']]
    passive_bitrate_dw = temp_df[temp_df.direction == 'dw'][['eventstamp','packetsize']]

    return passive_bitrate_dw, passive_bitrate_up


def plotActivePassiveBitrate(passive_bitrate, cap, node, direction):
    fig = plt.figure(figsize=(10,5))
    NODE_LIST = pkl.load(open(folder + 'NODE_LIST_FINAL.out', 'r'))
    ax1 = fig.add_subplot(1,1,1)
    temp = passive_bitrate.set_index('eventstamp').rename(columns = {'packetsize':'passive bitrate'})/(1024*1024)
    temp.plot(ax=ax1, color='g', marker='+', linestyle='None', legend=0)
    ax1.plot(cap[0], np.array(cap[1])*(128/(1024*1024)), color='k', linestyle='--', marker = 'o', label='active capacity')
    #ax1.legend(loc=3)
    ax1.grid(1)

    plt.ylabel('Measured Throughput (Mbps)', fontsize='x-large')
    plt.yticks(fontsize='x-large')
    plt.xticks(fontsize='x-large')
    print home_num[node], node, direction
    fig.savefig(outfolder + 'bitrate/' + str(home_num[node]) + '_' + node + '_' + direction + '.eps')

    #temp.save('passive_'+node+'_'+direction+'.pda')
    #pd.DataFrame({'eventstamp':cap[0], 'capacity':np.array(cap[1])*(128/(1024*1024))}).save('active_'+node+'_'+direction+'.pda')
    return


def loadData(folder, node):
    # [time stamps]
    active = pkl.load(open(folder + node + 'active.out', 'rb'))
    # ?
    device_state = pkl.load(open(folder + node + 'device_state.out', 'rb'))
    # device, port, direction, timehash : size
    bytesperportperminute = pkl.load(open(folder + node + 'bytesperportperminute.out', 'rb'))
    requestsperportperminute = pkl.load(open(folder + node + 'requestsperportperminute.out', 'rb'))
    # timesec, direction : size
    #bytespersecond = pkl.load(open(folder + node + 'bytespersecond.out', 'rb'))

    #now get active capacity using pgsql program in dp4~bismark-active/bitrate/
    #cap_up = zip(*pkl.load(open(folder + 'capacity201304/' + node + '_up.out', 'rb')))
    #cap_dw = zip(*pkl.load(open(folder + 'capacity201304/' + node + '_dw.out', 'rb')))
    return


def bps(bytespersecond):

    timestamp, direction = zip(*bytespersecond)
    packetsize = bytespersecond.values()

    df_bps = pd.DataFrame({'timestamp': timestamp, 'direction': direction,
                           'packetsize': packetsize}).sort(columns='eventstamp')
    df_bps['eventstamp'] = df_bps.timestamp.apply(lambda x: datetime.fromtimestamp(int(x)).replace(second=0))
    df_bps_unique = df_bps.set_index(['eventstamp', 'direction']).groupby(level=[0,1]).max()

    temp_df = df_bps_unique.reset_index()
    passive_bitrate_up = temp_df[temp_df.direction == 'up'][['eventstamp','packetsize']]
    passive_bitrate_dw = temp_df[temp_df.direction == 'dw'][['eventstamp','packetsize']]

    return


def linkUtilization(link_util, node):

    link_util['router'].append(node)
    link_util['home'].append(home_num[node])

    passive_bitrate_dw, passive_bitrate_up = loadPassiveBitrate(node)

    # take percentiles
    link_util['passive95_up'].append(np.percentile(passive_bitrate_up.packetsize, 95))
    link_util['passive95_dw'].append(np.percentile(passive_bitrate_dw.packetsize, 95))
    link_util['passive90_up'].append(np.percentile(passive_bitrate_up.packetsize, 90))
    link_util['passive90_dw'].append(np.percentile(passive_bitrate_dw.packetsize, 90))
    link_util['num_readings_up'].append(len(passive_bitrate_up))
    link_util['num_readings_dw'].append(len(passive_bitrate_dw))

    cap_dw, cap_up = loadActiveBitrate(node)

    # take 99% as active capacity
    link_util['active_dw'].append(np.percentile(np.array(cap_dw[1])*128, 99))
    link_util['active_up'].append(np.percentile(np.array(cap_up[1])*128, 99))

    #Now plot to compare
    plotActivePassiveBitrate(passive_bitrate_up, cap_up, node, 'up')
    plotActivePassiveBitrate(passive_bitrate_dw, cap_dw, node, 'dw')

    return


def main_bitrate():

    #datestart = 1364788800  # datetime(2013, 4, 4, 0, 0, 0).strftime(%s")
    #dateend = 1365998399    # datetime(2013, 4, 14, 23, 59, 59).strftime(%s")

    link_util = defaultdict(list)

    #for node in ['OW2CB05DA0D10B', 'OW100D7F64C8A3']:
    for node in NODE_LIST:
        linkUtilization(link_util, node)

    return pd.DataFrame(link_util)

if __name__ == "__main__":

    df_link = main_bitrate()

    #return 0
