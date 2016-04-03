# Task worker
# Connects PULL socket to tcp://localhost:5557
# Collects workloads from ventilator via that socket
# Connects PUSH socket to tcp://localhost:5558
# Sends results to sink via that socket
#
# Author: Lev Givon <lev(at)columbia(dot)edu>

import time
import zmq
import pickle

context = zmq.Context()

# Socket to receive messages on
receiver = context.socket(zmq.PULL)
receiver.connect("tcp://localhost:5557")

# Socket to send messages to
sender = context.socket(zmq.PUSH)
sender.connect("tcp://localhost:5558")

sentence = {}

# Process tasks forever
while True:
	cek = receiver.recv()

	print cek

	if cek == '':
		break
	elif (cek in sentence):
		sentence[cek] += 1
	else:
		sentence[cek] = 0


sort = sorted(sentence.items(), key=lambda x:x[1], reverse=True)
sort = pickle.dumps(sort)

# Send results to sink
sender.send(sort)

time.sleep(5)