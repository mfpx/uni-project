import socket
import struct
import sys
from schedule import *


message = b'very important data'
multicast_group = ('224.3.29.71', 10000)

@repeat(every(6).seconds)
def loop():
    # Create the datagram socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    # Set to non-blocking, but make sure threads wait for the function return
    # Broken, so use with caution
    #sock.setblocking(False)

    # Set a timeout so the socket does not block indefinitely when trying
    # to receive data.
    sock.settimeout(1)

    # Set the time-to-live for messages to 1 so they do not go past the
    # local network segment.
    ttl = struct.pack('b', 1)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)

    try:

        # Send data to the multicast group
        print('INFO: sending "%s"' % message.decode('utf-8'))
        sent = sock.sendto(message, multicast_group)
        print('INFO: socket returned {}'.format(sent)) # Socket return code

        # Look for responses from all recipients
        while True:
            print ('INFO: waiting to receive')
            try:
                data, server = sock.recvfrom(16)
            except socket.timeout:
                print('NOTICE: socket timeout reached, no more responses')
                break
            else:
                print('INFO: received "%s" from %s' % (data.decode('utf-8'), server))

    finally:
        print('NOTICE: end of cycle, closing socket')
        sock.close()

while True:
    run_pending()
    #time.sleep(1)