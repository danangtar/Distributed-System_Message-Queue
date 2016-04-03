# Task ventilator
# Binds PUSH socket to tcp://localhost:5557
# Sends batch of tasks to workers via that socket
#
# Author: Lev Givon <lev(at)columbia(dot)edu>

import zmq
import time
import os

try:
	raw_input
except NameError:
	# Python 3
	raw_input = input

context = zmq.Context()

# Socket to send messages on
sender = context.socket(zmq.PUSH)
sender.bind("tcp://*:5557")

# Socket with direct access to the sink: used to syncronize start of batch
sink = context.socket(zmq.PUSH)
sink.connect("tcp://localhost:5558")

print("Press Enter when the workers are ready: ")
_ = raw_input()
print("Sending tasks to workers ...")

# The first message is "0" and signals start of batch


sentence = {}

filenya = os.listdir("../filenya/.")
listnya = filenya

time.sleep(1)
for files in listnya:
	print files
	for line in open("../filenya/"+files).xreadlines():
		cek = line.split()
		cek = " ".join(cek[4:])
		time.sleep(0.001)
		sender.send(cek)

sender.send('')
sender.send('')


# Give 0MQ time to deliver
time.sleep(1)