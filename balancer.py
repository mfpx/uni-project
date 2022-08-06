import socket, asyncio, logging, struct, getopt, sys
from fcntl import ioctl
from balancerlibs.libserver import ServerLibrary, Helpers
from balancerlibs.libnet import LibIface
from balancerlibs.libsys import System
from schedule import *
# Load Balancer Core
# HIGHLY EXPERIMENTAL - NATURALLY POC ONLY!

log = logging.getLogger(__name__)
logging.basicConfig()

SIOCGIFADDR = 0x8915 # FCNTL DEVICE CONTROL CODE - DO NOT MODIFY
"""
Get, set, or delete the address of the device using
ifr_addr, or ifr6_addr with ifr6_prefixlen.  Setting or
deleting the interface address is a privileged operation.
For compatibility, SIOCGIFADDR returns only AF_INET
addresses, SIOCSIFADDR accepts AF_INET and AF_INET6
addresses, and SIOCDIFADDR deletes only AF_INET6
addresses.  A AF_INET address can be deleted by setting it
to zero via SIOCSIFADDR.

Source: netdevice.7 manpage
"""


class Balancer:


    # Selection algorithms: Lowest Latency (default), Round-Robin, ECMP (Equal Cost Multi Path)
    def __init__(self, type, pmtu, ifname = 'eth0', timeout = 30, algorithm = "latency"):
        self.type = type # Primary or Backup
        self.pmtu = pmtu # Packet MTU
        self.ifname = ifname # Local interface to use
        self.timeout = timeout
        self.algorithm = algorithm # Server selection algorithm


    def __call__(self):
        log.debug("Not yet implemented") # Do not call class directly
        

    def __setNodeConnectionTimeout(self, timeout) -> bool:
        """
        Sets the node connection timeout in milliseconds
        This determines how long the node will wait for a response
        during the initial connection attempt
        Once a connection is establised, this determines the liveness reply timeout period
        """
        self.timeout = timeout
        
        if self.timeout == timeout:
            return True
        else:
            return False


    def getNodeConnectionTimeout(self) -> int:
        """Returns the connection/liveness reply timeout period in milliseconds"""
        return self.timeout


    def __getType(self) -> str:
        """Returns the mode of operation - primary or backup"""
        return self.type

    
    def getMTU(self) -> int:
        """Returns the MTU (maximum transmission unit) of the current interface"""

        lif = LibIface(self.ifname)
        sysmtu = lif.getMTU()

        if sysmtu != self.pmtu:
            log.warn("Interface MTU modified externally! Expected {} but found {}".format(self.pmtu, sysmtu))
            # Maybe have CLI args to modify this behaviour?
            log.warn("MTU will now be reset to {} on interface {}".format(self.pmtu, self.ifname))
            lif.setMTU(self.pmtu)

        return sysmtu


    def setMTU(self, mtu) -> bool:
        """Sets MTU (maximum transmission unit) for the current interface"""

        # Set system MTU first
        lif = LibIface(self.ifname)
        newmtu = lif.setMTU(mtu)

        # Set instance MTU
        self.pmtu = mtu

        # Check and report
        if self.pmtu == newmtu:
            return True
        else:
            return False


    def __setInterface(self, interface) -> None:
        """Sets the interface to use for operation"""
        self.ifname = interface


    def __getInterface(self) -> str:
        """Returns the name of the current interface in use"""
        return self.ifname


    def setSelectionAlgorithm(self, algorithm) -> None:
        self.algorithm = algorithm


    def getSelectionAlgorithm(self) -> str:
        return self.algorithm


    class IPConfig:

        
        def __init__(self, name) -> None:
            self.name = name


        def __getLocalIP(self) -> str:
            """Returns a somewhat reliable LAN IP for the current node"""
            return socket.gethostbyname(socket.gethostname())


        def __getHostname(self) -> str:
            """
            Returns the node's hostname
            Note: This is the device name, not the LB node name
            """
            return socket.gethostname()


        def getInterfaceIP(self, interface) -> str:
            """Returns the IP address of a specific interface"""
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            return socket.intet_ntoa(ioctl
                                    (sock.fileno(),
                                    SIOCGIFADDR, # This shouldn't be a privileged request
                                    struct.pack(
                                        '256s',
                                        interface))[20:24]) # Struct takes char[]


        def setNodeName(self, name) -> bool:
            """Sets the named node name for use in the network"""
            self.name = name

            if self.name == name:
                return True
            else:
                return False

        
        def getNodeName(self) -> str:
            """Returns the named node name for use in the network"""
            return self.name


# Balancer -> Dest. Host -> Client
# REWORK USING ASYNCIO!
class ForwardEgress:


    def __init__(self, destination, destport) -> None:
        self.iface = iface
        self.bindip = bindip
        self.bindport = bindport
        self.destination = destination
        self.destport = destport


    def __socketOptions(self, sock, idle = 1, maxretrcount = 5, interval = 3, lingersecs = 5) -> None:
        """Sets socket options, also configures the socket for keep-alive"""
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, idle)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, interval)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, maxretrcount)
        #sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, lingersecs)


    # This will block but that's okay
    def forwardTraffic(self, data, writer, wait) -> bool:
        balancer = Balancer("primary", 1500)

        ingestServer = socket.socket(family = socket.AF_INET, type = socket.SOCK_STREAM, proto = 0)
        ingestServer.settimeout(balancer.getNodeConnectionTimeout())
        ingestServer.setblocking(True)

        # Socket options - Tested on kernel version 5.15.49
        self.__socketOptions(ingestServer, 1, 5, 3, 5)

        # Attempt to initiate connection and forward traffic, then return response to client
        print("DESTINATION", self.destination)
        print("PORT", self.destport)
        try:
            ingestServer.connect((self.destination, self.destport))
            if wait != False:
                state = ingestServer.sendall(data, socket.MSG_WAITALL)
                data = ingestServer.recv(2048, socket.MSG_WAITALL)
            else:
                state = ingestServer.sendall(data)
                data = ingestServer.recv(2048)

            size = Helpers().determinePayloadSize(data)
            log.warning(f"Server reply payload packet size is {size} bytes")

            writer.write(data)
            # await writer.drain() # Suspend until buffer is fully flushed
            writer.close()

            return state # If return is None, then request was successful
        except Exception as ex:
            raise # Reraise last exception


    async def checkNodeState(self) -> list | bool:
        slib = ServerLibrary()
        result = await slib.PingHost(self.destination, self.destport)

        if result == True:
            latencydata = slib.getLatencyToHost(self.destination)
            return True
        else:
            return False


# Client -> Balancer
class ForwardIngress:


    def __init__(self, interface = 'eth0', ip = '0.0.0.0', port = 8000):
        self.interface = interface
        self.bindip = ip
        self.port = port

        self.dest = 0
        self.destport = 80

    @repeat(every(5).seconds)
    def setHost(self):
        balancer = Balancer("primary", 1500, "enp4s0")
        print("repeat got executed!")

        # Set host
        if balancer.getSelectionAlgorithm() == "latency":
            print("Determining the best host")
            host = ServerLibrary().pickLatencyHost()

            self.dest = host['host']
            self.destport = 80 # Add this to config
            log.warning(f"Selected \"{self.dest}\" as the destination")
        

    async def ingressServer(self):
        flags = [socket.SOL_SOCKET, socket.SO_REUSEADDR]
        server = await asyncio.start_server(self.response, self.bindip, self.port, family = socket.AF_INET, flags = flags)

        async with server:
            await server.serve_forever()

    
    async def response(self, reader, writer): # NOTE: writer will write data back to client in this instance
        data = await reader.read(2048)

        balancer = Balancer("primary", 1500, "enp4s0")

        size = Helpers().determinePayloadSize(data)
        log.warning(f"Client payload packet size is {size} bytes")

        # --- UGLY CODE BEGIN ---
        newdata = data.decode('utf-8').split(' ')
        newdata[3] = f'{self.dest}:{self.destport}\r\n' # Look for an alt solution - hardcoding is bad

        bytestring = ''
        for item in newdata:
            bytestring += item + ' '

        data = bytestring.encode()
        # --- UGLY CODE END ---

        if size > balancer.getMTU():
            log.warning("Packet size is greater than interface MTU! It will now be changed to prevent fragmentation")
            balancer.setMTU(size)

        traffic = ForwardEgress(self.dest, self.destport).forwardTraffic(data, writer, True)

        if traffic != None:
            log.error("The request failed!")

#run_pending()
#fi = ForwardIngress()
#asyncio.run(fi.ingressServer())

if __name__ == "__main__":
    system = System()

    try:
        argv = sys.argv[1:]
        opts, args = getopt.getopt(argv, "hm:ni:", ["staticmtu=", "iface=", "nomtu"])
        if len(opts) != 0:
            system.cli(opts)
    except getopt.GetoptError:
        print("balancer.py [hmni] --staticmtu= --iface= --nomtu")
