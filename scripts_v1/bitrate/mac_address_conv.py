import pickle as pkl

NODE_LIST = pkl.load(open('/home/sarthak/bismark-passive/scripts/NODE_LIST.dat','rb'))

def convertMAC(node):
    p = node[2:]
    d = p.lower()
    f = ':'.join([d[i:i+2] for i in range(0, len(d), 2)])
    return f