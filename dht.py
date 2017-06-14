import sys
import random
import hashlib
import argparse
import pandas as pd
import numpy as np
from rdflib.graph import Graph
import glob

k=255
N   = 1000
dht = None
MAX = None
B=False

class Node:
    def __init__(self, id, ip=None, port=None):
        self.id = id
        self.ip = ip
        self.port = port
        self.long_id = long(id.encode('hex'), 32)
        self.next = None
        self.count = 0

        self.left  = None
        self.right = None

        self.charLeft = ord('a')
        self.charRight = ord('b')


    def distanceTo(self, node):
        return self.long_id ^ node.long_id

    def up(self,otherNode=None,string=None):
        if string:
            if ord(string[0]) > self.charRight:
                print "izq"
            else:
                print "der"
        self.count += 1
        if B:
            if otherNode:
                self.count += otherNode
            if self.count > MAX:
                print "realizando split"
                self.split()
                self.count = 0

    def split(self):
        leftId = Node(digest(random.getrandbits(255)))
        rightId = Node(digest(random.getrandbits(255)))
        self.left  = findNode(dht,leftId)
        self.right = findNode(dht,rightId)
        print "El nodo left quedara con %s " %(self.left.count+MAX/2)
        print "El nodo right quedara con %s " %(self.right.count+MAX/2)


def findNode(start, key):
    current=start
    while distance(current, key) > distance(current.next, key):
        current=current.next
    return current

def distance(a, b):
    a = a.long_id
    b = b.long_id
    if a==b:
        return 0
    elif a<b:
        return b-a;
    else:
        return (2**k)+(b-a);

def insertAllNodes(n):
    for i in range(n):
        rid = hashlib.sha1(str(random.getrandbits(255))).digest()
        n = Node(rid)
        responsable = findNode(dht,n)
        tmp = responsable.next
        responsable.next = n
        n.next = tmp

def insertData(data,second=None):
    if second:
        rid = hashlib.sha1(str(data)+str(second)).digest()
    else:
        rid = hashlib.sha1(str(data)).digest()
    dataNode = Node(rid)
    responsable = findNode(dht,dataNode)
    #print data.split('/')[-1]
    responsable.up()

def digest(s):
    if not isinstance(s, str):
        s = str(s)
    return hashlib.sha1(s).digest()

def bootstrap():
    dht = Node(digest(random.getrandbits(255)))
    b = Node(digest(random.getrandbits(255)))
    dht.next = b
    b.next = dht
    return dht

def readTriples(file):
    triples = [line[:-1] for line in file]
    for t in triples:
        #print t
        insertData(t)

def parse(path):
    g = Graph()
    triple = {}
    files = path + "/*.ttl"
    print files
    for filename in glob.glob(files):
        print filename
        g.parse(filename, format="nt")
        for subject,predicate,obj_ in g:
           assert (subject,predicate,obj_) in g, "Iterator / Container Protocols are Broken!!"
           insertData(subject.encode('utf-8'))
           insertData(predicate.encode('utf-8'))
           insertData(obj_.encode('utf-8'))

def parseMix(path):
    print "mixed"
    g = Graph()
    triple = {}
    files = path + "/*.ttl"
    print files
    for filename in glob.glob(files):
        print filename
        g.parse(filename, format="nt")
        for subject,predicate,obj_ in g:
           assert (subject,predicate,obj_) in g, "Iterator / Container Protocols are Broken!!"
           insertData(subject.encode('utf-8'),predicate.encode('utf-8'))
           insertData(predicate.encode('utf-8'),obj_.encode('utf-8'))
           insertData(obj_.encode('utf-8'),subject.encode('utf-8'))

parser = argparse.ArgumentParser()
parser.add_argument("triples", help="display a square of a given number",type=str)
parser.add_argument('-m', action='store_true', default=False, dest='mixed')
parser.add_argument('-b',
                    dest='btree',
                    default=0,
                    action='store',
                    nargs='?',
                    type=int)

args = parser.parse_args()
print args
if args.btree > 0:
    B = True
    MAX = args.btree

dht =  bootstrap()
insertAllNodes(N)

if args.mixed:
    print "Parsear con metodos mix"
    parseMix(args.triples)
else:
    print "Parsear con el metodo propuesto"
    parse(args.triples)

data = []
cursor = dht
while dht != cursor.next:
    #print cursor.long_id
    cursor = cursor.next
    data.append(cursor.count)
df = pd.DataFrame(np.asarray(data), columns=['triples'])
csvOutput = "%s-%s-%s.csv" %(args.triples,args.btree,args.mixed)
df.to_csv(csvOutput, index=False)
