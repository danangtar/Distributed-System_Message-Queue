# Task sink
# Binds PULL socket to tcp://localhost:5558
# Collects results from workers via that socket
#
# Author: Lev Givon <lev(at)columbia(dot)edu>

import sys
import time
import zmq
import pickle

context = zmq.Context()

# Socket to receive messages on
receiver = context.socket(zmq.PULL)
receiver.bind("tcp://*:5558")

# Wait for start of batch

# Start our clock now
tstart = time.time()

terima = receiver.recv()
hasil1 = pickle.loads(terima)

terima = receiver.recv()
hasil2 = pickle.loads(terima)
hasil2 = dict((x, y) for x, y in hasil2)

for freq in hasil1:
    if freq[0] in hasil2:
        hasil2[freq[0]] = hasil2[freq[0]] + freq[1]
    else:
        hasil2[freq[0]] = freq[1]

hasil2 = sorted(hasil2.items(), key=lambda x: x[1], reverse=True)

i = 0
while i < 10:
    print hasil2[i]
    i = i + 1

# Calculate and report duration of batch
tend = time.time()
print("Total elapsed time: %d msec" % ((tend-tstart)*1000))
