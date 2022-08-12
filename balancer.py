import socket, asyncio, logging, struct, getopt, sys
import concurrent.futures
from fcntl import ioctl
import threading
from balancerlibs.libserver import ServerLibrary, Helpers
from balancerlibs.libnet import LibIface
from schedule import *
from datetime import datetime
# Load Balancer Core
# HIGHLY EXPERIMENTAL - NATURALLY POC ONLY!

log = logging.getLogger(__name__)
logging.basicConfig(filename="balancer.log", level=logging.DEBUG)

SIOCGIFADDR = 0x8915 # FCNTL SOCKET CONFIGURATION CONTROL CODE - DO NOT MODIFY (see sockios.h)
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
    """Main balancer class"""


    # Selection algorithms: Lowest Latency (default), Round-Robin
    def __init__(self, type: str, pmtu: int, ifname = 'eth0', timeout = 30, algorithm = "latency", name = "balancer"):
        self.type = type # Primary or Backup
        self.pmtu = pmtu # Packet MTU
        self.ifname = ifname # Local interface to use
        self.timeout = timeout
        self.algorithm = algorithm # Server selection algorithm
        self.name = name # Balancer node name
        self.static_mtu = False # Use static MTU
        self.no_mtu = False # Do not change interface MTU at all


    def __call__(self):
        log.debug("Not implemented") # Do not call class directly, used for control sockets (unimplemented)
        # Return balancer configuration and stats to controlling clients

    
    def useStaticMTU(self, state: bool) -> None:
        """Sets instance to use static MTU"""
        self.static_mtu = state

    
    def noMTUChange(self, state: bool) -> None:
        """Prevents instance from changing interface MTU"""
        self.no_mtu = state

    
    def setNodeName(self, name: str) -> None:
        """Sets node name in multinode setups"""
        self.name = name


    def getNodeName(self) -> str:
        """Gets node name for multinode setups"""
        return self.name


    def bootstrap(self, cfg) -> bool:
        self.cfg = cfg
        slib = ServerLibrary(log)

        log.info(f"--- Bootstrapping started at {datetime.now().strftime('%H:%M:%S on %x')} ---")
        if cfg["bindip"] == "lan":
            ifip = self.getInterfaceIP(self.getInterface())
            log.info(f"Interface {self.getInterface()} has IP {ifip}")
        else:
            ifip = cfg["bindip"]
            log.info(f"Configured IP is {ifip}")

        if self.getNodeName() == "auto" or self.getNodeName() == "":
            self.setNodeName(Helpers().generateNodeName())

        log.info(f"Node name is set to {self.getNodeName()}")
        print(f"Node name is set to {self.getNodeName()}")

        if self.static_mtu == True and self.no_mtu == True:
            log.error("Do not use --staticmtu and --nomtu together")
            sys.exit(1)
        elif self.static_mtu == True:
            log.info(f"INFO: This node will use a static MTU of {self.pmtu}")
        elif self.no_mtu == True:
            log.info(f"INFO: This node will not attempt to change {self.ifname} MTU")
        else:
            self.setMTU(self.pmtu)
            log.info(f"MTU of {self.getInterface()} set to {self.pmtu}")
        
        if self.getSelectionAlgorithm() == "latency":
            log.info(f"- Upstream node latency test started at {datetime.now().strftime('%H:%M:%S on %x')} -")
            latencyhost = slib.pickLatencyHost()
            host, port = latencyhost["host"].split(':')
            log.info(f"- Upstream node latency test ended at {datetime.now().strftime('%H:%M:%S on %x')} -")
        elif self.getSelectionAlgorithm() == "roundrobin":
            host, port = cfg["servers"][0].split(':')
            log.info(f"First host will be {host}:{port}")


        fi = ForwardIngress(
         self,
         self.getInterface(),
         ifip,
         cfg["bindport"])
        
        fi.setInitHost(host, port)

        log.info(f"--- Bootstrapping finished at {datetime.now().strftime('%H:%M:%S on %x')} ---")
        self.runThreaded(fi.setHost)

        asyncio.run(fi.ingressServer())


    def runThreaded(self, job_func):
        """Runs a function in a new thread"""
        job_thread = threading.Thread(target = job_func)
        job_thread.start()

        
    def setNodeConnectionTimeout(self, timeout: int) -> bool:
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


    def getType(self) -> str:
        """Returns the mode of operation - primary or backup"""
        return self.type

    
    def getMTU(self) -> int:
        """Returns the MTU (maximum transmission unit) of the current interface"""

        lif = LibIface(self.ifname)
        sysmtu = lif.getMTU()

        if sysmtu != self.pmtu:
            log.warning("Interface MTU modified externally! Expected {} but found {}".format(self.pmtu, sysmtu))

        return sysmtu


    def setMTU(self, mtu: int) -> bool:
        """Sets MTU (maximum transmission unit) for the current interface"""

        lif = LibIface(self.ifname)

        if self.no_mtu == False and self.static_mtu == False:
            # Set system MTU first
            newmtu = lif.setMTU(mtu)
            # Set instance MTU
            self.pmtu = newmtu
        elif self.static_mtu == True:
            newmtu = lif.setMTU(self.pmtu)
        else:
            return 2

        # Check and report
        if self.pmtu == lif.getMTU():
            return True
        else:
            return False


    def setInterface(self, interface: str) -> None:
        """Sets the interface to use for operation"""
        self.ifname = interface


    def getInterface(self) -> str:
        """Returns the name of the current interface in use"""
        return self.ifname


    def setSelectionAlgorithm(self, algorithm: str) -> None:
        self.algorithm = algorithm


    def getSelectionAlgorithm(self) -> str:
        return self.algorithm

    
    def getHostname(self) -> str:
        """
        Returns the node's hostname
        Note: This is the device name, not the LB node name
        """
        return socket.gethostname()


    def getInterfaceIP(self, interface: str) -> str:
        """Returns the IP address of a specific interface"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        return socket.inet_ntoa(ioctl
                                (sock.fileno(),
                                SIOCGIFADDR, # This shouldn't be a privileged request
                                struct.pack(
                                    '256s',
                                    bytes(interface.encode("utf-8"))))[20:24]) # Struct takes char[]


    def setNodeName(self, name: str) -> None:
        """Sets the named node name for use in the network"""
        self.name = name


    def getNodeName(self) -> str | None:
        """Returns the named node name for use in the network"""
        return self.name


# Balancer -> Dest. Host -> Client
# REWORK USING ASYNCIO!
class ForwardEgress:


    def __init__(self, destination: str, destport: int, balancer: Balancer) -> None:
        self.balancer = balancer
        self.destination = destination
        self.destport = destport


    def __socketOptions(self, sock: socket, idle = 1, maxretrcount = 5, interval = 3, lingersecs = 5) -> None:
        """Sets socket options, also configures the socket for keep-alive"""
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, idle)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, interval)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, maxretrcount)
        #sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, lingersecs)


    # This will block but that's okay
    def forwardTraffic(self, data, writer, wait) -> bool:

        ingestServer = socket.socket(family = socket.AF_INET, type = socket.SOCK_STREAM, proto = 0)
        ingestServer.settimeout(self.balancer.getNodeConnectionTimeout())
        ingestServer.setblocking(True) # Non-blocking raises EINPROGRESS (see errno.h)

        # Socket options - Tested on kernel version 5.15.49 and 5.18.0
        self.__socketOptions(ingestServer, 1, 5, 3, 5)

        # Attempt to initiate connection and forward traffic, then return response to client
        try:
            ingestServer.connect((self.destination, self.destport))
            if wait != False:
                state = ingestServer.sendall(data, socket.MSG_WAITALL)
                data = b''
                while True:
                    datastream = ingestServer.recv(2048, socket.MSG_WAITALL)
                    if len(datastream) <= 0:
                        break
                    data += datastream
            else:
                state = ingestServer.sendall(data)
                data = ingestServer.recv(2048)

            size = Helpers().determinePayloadSize(data)
            log.debug(f"Server reply payload packet size is {size} bytes")

            if size > self.balancer.getMTU():
                log.warning("Packet size is greater than interface MTU! It will now be changed to prevent fragmentation")
                self.balancer.setMTU(size)

            writer.write(data)
            # await writer.drain() # Suspend until buffer is fully flushed
            writer.close()

            return state # If return is None, then request was successful
        except Exception as ex:
            raise # Reraise last exception


    async def checkNodeState(self) -> list | bool:
        slib = ServerLibrary(log)
        result = await slib.PingHost(self.destination, self.destport)

        if result == True:
            latencydata = slib.getLatencyToHost(self.destination)
            return True
        else:
            return False


# Client -> Balancer
class ForwardIngress:


    def __init__(self, balancer: Balancer, interface = 'eth0', ip = '0.0.0.0', port = 8000):
        self.interface = interface
        self.bindip = ip
        self.port = port
        self.balancer = balancer
        self.initParamsSet = False


    def setInitHost(self, host: str, port: int) -> bool | None:
        if self.initParamsSet == False:
            self.dest = host
            self.destport = int(port)
            self.initParamsSet = True
        else:
            return False


    def setHost(self):
        time.sleep(30) # Maybe use the schedule lib?

        # Set host
        if self.balancer.getSelectionAlgorithm() == "latency":
            log.info("Determining the best host")
            host = ServerLibrary(log).pickLatencyHost()

            hostip, hostport = host["host"].split(':')

            self.dest = hostip
            self.destport = int(hostport)

            log.info(f"Selected \"{self.dest}:{self.destport}\" as the destination")
        elif self.balancer.getSelectionAlgorithm() == "roundrobin":
            lastHost = self.dest + ':' + str(self.destport)
            host = ServerLibrary(log).pickRoundRobinHost(lastHost)

            hostip, hostport = host.split(':')

            self.dest = hostip
            self.destport = int(hostport)

            log.info(f"Selected \"{self.dest}:{self.destport}\" as the destination")

        self.setHost()
        

    async def ingressServer(self):
        flags = [socket.SOL_SOCKET, socket.SO_REUSEADDR]
        server = await asyncio.start_server(self.response, self.bindip, self.port, family = socket.AF_INET, flags = flags)

        async with server:
            await server.serve_forever()

    
    async def response(self, reader, writer): # NOTE: writer will write data back to client in this instance
        """        data = b''
                while True:
                    datastream = await reader.read(2048)
                    print("DATA LENGTH IS", len(datastream))
                    if len(datastream) <= 0:
                        print("LENGTH IS 0 NOW")
                        break
                    data += datastream"""

        data = await reader.read(2048)

        size = Helpers().determinePayloadSize(data)
        log.debug(f"Client payload packet size is {size} bytes")

        # This will replace host address header
        # --- UGLY CODE BEGIN ---
        try:
            # This will match the host and port
            hostRegex = "Host: (?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?):([1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])"
            bytestring = re.sub(hostRegex, f'Host: {self.dest}:{self.destport}', data.decode('utf-8'), count = 1)
            #print(bytestring)

            data = bytes(bytestring.encode('utf-8'))
        # --- UGLY CODE END ---

            if size > self.balancer.getMTU():
                log.warning("Packet size is greater than interface MTU! It will now be changed to prevent fragmentation")
                self.balancer.setMTU(size)

            traffic = ForwardEgress(self.dest, self.destport, self.balancer).forwardTraffic(data, writer, True)

            if traffic != None:
                log.error("The request failed!")
        except:
            log.warning("Non-HTTP request received! Ignoring.")
            raise

#run_pending()
#fi = ForwardIngress()
#asyncio.run(fi.ingressServer())

if __name__ == "__main__":
    helpers = Helpers()
    cfg = helpers.ConfigParser()

    balancer = Balancer(
    cfg["node_type"], # Primary or Backup
    cfg["default_pmtu"], # Default MTU value
    cfg["ifname"], # Interface name
    cfg["timeout"], # Connection timeout
    cfg["algorithm"], # Round-robin or latency
    cfg["hostname"]) # Node name

    try:
        argv = sys.argv[1:]
        opts, args = getopt.getopt(argv, "hm:ni:c:", ["staticmtu=", "iface=", "conf=", "nomtu"])
        if len(opts) != 0:
            for opt, arg in opts:
                if opt == '-h':
                    print("balancer.py [hmicn] --staticmtu= --iface= --conf= --nomtu")
                    sys.exit(1)
                elif opt in ("-i", "--iface"):
                    balancer.setInterface(arg)
                elif opt in ("-m", "--staticmtu"):
                    balancer.setMTU(int(arg))
                    balancer.useStaticMTU(True)
                elif opt in ("-n", "--nomtu"):
                    balancer.noMTUChange(True)
                elif opt in ("-c", "--conf"):
                    cfg = helpers.ConfigParser(arg)
    except getopt.GetoptError:
        print("Unknown argument\nbalancer.py [hmicn] --staticmtu= --iface= --conf= --nomtu")
        sys.exit(1)

    with concurrent.futures.ProcessPoolExecutor(max_workers = 5) as executor:
        future = executor.submit(balancer.bootstrap, cfg)
