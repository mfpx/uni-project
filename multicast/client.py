import socket
import struct
import sys
import scapy.supersocket as supersocket

"""
Responses:
ack - packet received and will be processed
nak - packet received but will not be processed (TO DO)
malformed - packet received, but the payload was not understood (TO DO)
"""

# TODO: multiple multicast groups in case others are occupied - add this to config
# how to test for occupancy?
multicast_group = '224.3.29.71'
server_address = ('', 10000)

# Create the socket
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# Bind to the server address
sock.bind(server_address)

# Tell the operating system to add the socket to the multicast group
# on all interfaces.
group = socket.inet_aton(multicast_group)
mreq = struct.pack('4sL', group, socket.INADDR_ANY)
sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

# Receive/respond loop
while True:
    print('\nINFO: waiting to receive message')
    data, address = sock.recvfrom(1024)
    print('INFO: received %s bytes from %s' % (len(data), address))
    print(data.decode('utf-8'))

    print('INFO: sending acknowledgement to', address)
    sock.sendto(b'ack', address)

