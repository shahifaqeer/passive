#!/usr/bin/python
import pickle as pkl
from gzip import GzipFile as gz
import pg as pgsql
import sys
import traceback
import os
import random as rnd
import socket, struct
import numpy as np
from mac_address_conv import convertMAC


sql_host = open('credentials/pghost_file').readline().split('\n')[0]
sql_user = open('credentials/pguser_file').readline().split('\n')[0]
sql_passwd = open('credentials/pgpasswd_file').readline().split('\n')[0]
sql_db = open('credentials/pgdb_file').readline().split('\n')[0]

def sqlconn():
  try:
    conn = pgsql.connect(dbname=sql_db,host=sql_host,user=sql_user,passwd=sql_passwd)
    #cursor = conn.cursor()
  except:
    print "Could not connect to sql server"
    sys.exit()
  return conn

def run_insert_cmd(cmd,conn=None,prnt=0):
  if conn == None:
    conn = sqlconn()
  #print cmd
  try:
    conn.query(cmd)
  except:
    print "Couldn't run %s\n"%(cmd)
    return 0
  #cursor.fetchall()
  return 1

def run_data_cmd(cmd,conn=None,prnt=0):
  if conn == None:
    conn = sqlconn()
  res = ''
  if prnt == 1:
    print cmd
  try:
    res = conn.query(cmd)
  except:
    #print "Couldn't run %s\n"%(cmd)
    return 0
  result = res.getresult()
  return result


if __name__ == '__main__':
  #cmd = "SELECT traceroutes.deviceid, traceroutes.srcip, traceroutes.dstip, traceroutes.eventstamp, m_bitrate.eventstamp, m_bitrate.average, traceroutes.hops, traceroute_hops.hop, traceroute_hops.rtt, traceroute_hops.ip FROM m_bitrate JOIN traceroutes ON (traceroutes.deviceid = m_bitrate.deviceid AND traceroutes.srcip=m_bitrate.srcip AND traceroutes.dstip=m_bitrate.dstip AND traceroutes.eventstamp = m_bitrate.eventstamp), traceroute_hops WHERE traceroutes.id = traceroute_hops.id LIMIT 10;"
  cmd = "SELECT r.deviceid, r.srcip, r.dstip, r.eventstamp, m.eventstamp, m.average, r.hops, h.hop, h.rtt, h.ip FROM m_bitrate m JOIN traceroutes r ON (r.deviceid = m.deviceid AND r.srcip=m.srcip AND r.dstip=m.dstip AND r.eventstamp = m.eventstamp), traceroute_hops h WHERE r.id = h.id LIMIT 2;"
  myquery = run_data_cmd(cmd);
  print "CHECK"
  print myquery

  NODE_LIST = pkl.load(open('/home/sarthak/bismark-passive/scripts/NODE_LIST.dat','rb'))
  for node in NODE_LIST:
      for direction in ['up','dw']:
          dev = convertMAC(node)
          #cmd3 = "SELECT deviceid, srcip, dstip, eventstamp, average FROM m_bitrate WHERE toolid = 'NETPERF_3' AND date(eventstamp)>'2012-10-01' AND date(eventstamp)<'2012-10-15' AND direction = '"+ direction +"' AND deviceid = '" + dev + "';"
          #myquery3 = run_data_cmd(cmd3)
          #pkl.dump(myquery3, open('throughput/'+node+'_'+direction+'.out', 'wb'))
          cmd2 = "SELECT deviceid, srcip, dstip, eventstamp, average FROM m_capacity WHERE date(eventstamp)>'2013-04-01' AND date(eventstamp)<'2012-04-15' AND direction = '"+ direction +"' AND deviceid = '" + dev + "';"
          myquery2 = run_data_cmd(cmd2)
          pkl.dump(myquery2, open('capacity/'+node+'_'+direction+'.out', 'wb'))


  print "DONE"
