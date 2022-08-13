import socket, os, sys, struct, json, inspect, base64
from schedule import *

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir) 

from encryption.jsoncrypt import CryptoLib

class Advertise:
    def __init__(self, name: str, node_type: str, mcast_group: tuple) -> None:
        self.name = name
        self.node_type = node_type
        self.timeout = 1
        self.mcast_group = mcast_group


    def set_sock_timeout(self, timeout: int) -> None:
        self.timeout = timeout


    def get_sock_timeout(self) -> int:
        return self.timeout


    def create_socket(self) -> Callable:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(1)
        ttl = struct.pack('b', self.get_sock_timeout())
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
        return sock


    def __validate_type(self):
        if self.node_type in ("primary", "backup"):
            return True
        else:
            return False

    
    def create_message(self, msgdata: dict, password: str | int) -> bytes:
        msgdata["name"] = self.name
        if self.__validate_type():
            msgdata["node_type"] = self.node_type
        else:
            return -1

        jsonmsg = json.dumps(msgdata, indent = 4)
        crypto = CryptoLib(password)
        message = crypto.encrypt_json(jsonmsg)
        return base64.b64encode(str(message).encode())


    def mcast_message(self, msg: bytes, socket: socket):
        sent = socket.sendto(msg, self.mcast_group)
        print(f"Socket returned {sent}")

        while True:
            try:
                data, server = socket.recvfrom(16)
            except TimeoutError:
                print("Socket timeout reached")
                break
            else:
                socket.close()
                return {"data": data, "server": server}


# https://www.iana.org/assignments/multicast-addresses/multicast-addresses.xhtml
adv = Advertise("node", "primary", ("224.3.29.71", 10000))
sock = adv.create_socket()
msg = adv.create_message({"hello": "world"}, "password")

print(msg)

adv.mcast_message(msg, sock)
