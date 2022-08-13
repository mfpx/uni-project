import socket, struct, json

"""
Responses:
ack - packet received and will be processed
nak - packet received but will not be processed (TO DO)
malformed - packet received, but the payload was not understood (TO DO)
"""

class MulticastRx:
    def __init__(self, mcast_group: tuple) -> None:
        self.mcast_group = mcast_group
        self.timeout = 1


    def set_timeout(self, timeout: int) -> None:
        """Sets the socket timeout"""
        self.timeout = timeout


    def create_socket(self) -> socket:
        """Creates a socket and sets its options for multicasting"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(self.timeout)
        sock.bind(self.mcast_group)
        group = socket.inet_aton(self.mcast_group[0])
        mreq = struct.pack('4sL', group, socket.INADDR_ANY) # All interfaces
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

        return sock

    # TODO: Add stricter validation
    def __validate(self, input: bytes) -> dict | bool:
        """Performs rudimentary message validation"""
        try:
            data = json.loads(input.decode('utf-8'))
            return data
        except:
            return False


    def listen(self, sock: socket) -> dict | bool:
        """Listens for multicast data"""
        try:
            data, address = sock.recvfrom(1024)
        except TimeoutError:
            return -1
        validate = self.__validate(data)
        if validate == False:
            sock.sendto(b'malformed', address)
            return False
        else:
            sock.sendto(b'ask', address)
            return {"data": data, "address": address}


mcast = MulticastRx((("224.3.29.71", 10000)))
mcast.set_timeout(30)
sock = mcast.create_socket()
data = mcast.listen(sock)

print(data)
