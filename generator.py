#!/usr/bin/python3.6

import random
import datetime
import getopt
import sys


def generate(clients):
    date = datetime.datetime(random.randint(2018,2019), random.randint(1,12), random.randint(1,28), random.randint(0,23), random.randint(0,59), random.randint(0,59))
    return "%s - - [%s +0100] \"GET https://google.com\" 400 %s \"-\"%s" %(
        "%s.%s.%s.%s" % (random.randint(1,254), random.randint(1,254), random.randint(1,254), random.randint(1,254)),
        date.strftime("%d/%b/%Y:%H:%M:%S"),
        random.randint(50, 5000),
        random.choice(clients)
    )

def main():
    opts, args = getopt.getopt(sys.argv[1:], "o:s:c:", [ "output=", "size=", "count="])
    output = None
    size = 0
    count = 0
    for o, a in opts:
        if o in ("-s", "--size"):
            size = int(a)
        elif o in ("-o", "--output"):
            output = a
        elif o in ("-c", "--count"):
            count = int(a)
        else:
            print("unhandled option")

    agents = []

    with open("clients.txt", 'r') as fp:
        agents = fp.read().splitlines()

    limit_size = size*1024*1024
    for i in range(0, count):
        sum_size = 0
        f = open(output+"_"+str(i)+".log", "w")
        while sum_size < limit_size:
            line = generate(agents)
            sum_size += len(line)+1
            f.write(line+'\n')
        f.close()

if __name__ == "__main__":
    main()
