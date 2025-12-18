import socket, asyncio, logging, struct, getopt, sys, deprecation
import re
import concurrent.futures
from fcntl import ioctl
import threading
from balancerlibs.libserver import ServerLibrary, Helpers
from balancerlibs.libnet import LibIface
import schedule
from datetime import datetime
import time
from multicast.mcast import MulticastTx, MulticastRx
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
        self.multicast_group = "" # Multicast group IP
        self.multicast_port = 0 # Multicast group port
        self.multicast_password = "" # Multicast encryption password


    def __call__(self):
        log.debug("Not implemented") # Do not call class directly, used for control sockets (unimplemented)
        # Return balancer configuration and stats to controlling clients

    
    def use_static_mtu(self, state: bool) -> None:
        """Sets instance to use static MTU"""
        self.static_mtu = state

    
    def no_mtu_change(self, state: bool) -> None:
        """Prevents instance from changing interface MTU"""
        self.no_mtu = state

    
    def set_node_name(self, name: str) -> None:
        """Sets node name in multinode setups"""
        self.name = name


    def get_node_name(self) -> str:
        """Gets node name for multinode setups"""
        return self.name


    def set_multicast_group(self, group: str) -> None:
        self.multicast_group = group


    def get_multicast_group(self) -> str:
        return self.multicast_group


    def set_multicast_password(self, password) -> None:
        self.multicast_password = password

    
    def get_multicast_password(self) -> str:
        return self.multicast_password


    def set_multicast_port(self, port: int) -> None:
        self.multicast_port = port


    def get_multicast_port(self) -> int:
        return self.multicast_port


    async def shutdown(self, loop):
        loop.close()
        await loop.wait_closed()

    # https://schedule.readthedocs.io/en/stable/background-execution.html
    def run_continuously(self, interval=1):
        """Continuously run, while executing pending jobs at each
        elapsed time interval.
        @return cease_continuous_run: threading. Event which can
        be set to cease continuous run. Please note that it is
        *intended behavior that run_continuously() does not run
        missed jobs*. For example, if you've registered a job that
        should run every minute and you set a continuous run
        interval of one hour then your job won't be run 60 times
        at each interval but only once.
        """
        cease_continuous_run = threading.Event()

        class ScheduleThread(threading.Thread):
            @classmethod
            def run(cls):
                while not cease_continuous_run.is_set():
                    schedule.run_pending()
                    time.sleep(interval)

        continuous_thread = ScheduleThread()
        continuous_thread.start()
        return cease_continuous_run


    def heartbeat(self):
        mcast_group = (self.get_multicast_group(), self.get_multicast_port())
        if self.get_type() == "primary":
            mtx = MulticastTx(self.get_node_name(), "primary", mcast_group)
            mtx.set_sock_timeout(10) # This must happen before a socket instance is created
            sock = mtx.create_socket()
            ts = datetime.now().strftime('%H:%M:%S.%f')
            msgdata = {"heartbeat": ts}
            tx = mtx.create_message(msgdata, self.get_multicast_password())
            recv = mtx.mcast_message(tx, sock)

            if recv is not None:
                if recv['data'] == b'ack':
                    recvtime = datetime.now().strftime('%H:%M:%S.%f')
                    diff = datetime.strptime(recvtime, '%H:%M:%S.%f') - datetime.strptime(ts, '%H:%M:%S.%f')
                    log.debug(f"Heartbeat acknowledged by {recv['server']} in {diff.microseconds / 1000}ms")
            else:
                log.debug("Heartbeat not acknowledged by any node")


    async def heartbeat_loop(self, interval: int) -> None:
        """Async loop that runs the blocking heartbeat in a thread every `interval` seconds"""
        while True:
            await asyncio.to_thread(self.heartbeat)
            await asyncio.sleep(interval)


    def bootstrap(self, cfg):
        self.cfg = cfg
        slib = ServerLibrary(log)

        log.info(f"--- Bootstrapping started at {datetime.now().strftime('%H:%M:%S on %x')} ---")
        if cfg["bindip"] == "lan":
            ifip = self.get_interface_ip(self.get_interface())
            log.info(f"Interface {self.get_interface()} has IP {ifip}")
        else:
            ifip = cfg["bindip"]
            log.info(f"Configured IP is {ifip}")

        if self.get_node_name() == "auto" or self.get_node_name() == "":
            self.set_node_name(Helpers().generate_node_name())

        log.info(f"Node name is set to {self.get_node_name()}")
        print(f"Node name is set to {self.get_node_name()}")

        self.set_multicast_group(cfg["multicast_group"])
        log.debug(f"Multicast group set to {self.get_multicast_group()}")
        self.set_multicast_port(cfg["multicast_port"])
        log.debug(f"Multicast port set to {self.get_multicast_port()}")
        self.set_multicast_password(cfg["network_password"])

        if self.static_mtu == True and self.no_mtu == True:
            log.error("Do not use --staticmtu and --nomtu together")
            sys.exit(1)
        elif self.static_mtu == True:
            log.info(f"This node will use a static MTU of {self.pmtu}")
        elif self.no_mtu == True:
            log.info(f"This node will not attempt to change {self.ifname} MTU")
        else:
            self.set_mtu(self.pmtu)
            log.info(f"MTU of {self.get_interface()} set to {self.pmtu}")
        
        if self.get_selection_algorithm() == "latency":
            log.info(f"- Upstream node latency test started at {datetime.now().strftime('%H:%M:%S on %x')} -")
            latencyhost = slib.pick_latency_host()
            host, port = latencyhost["host"].split(':')
            log.info(f"- Upstream node latency test ended at {datetime.now().strftime('%H:%M:%S on %x')} -")
        elif self.get_selection_algorithm() == "roundrobin":
            host, port = cfg["servers"][0].split(':')
            log.info(f"First host will be {host}:{port}")

        fi = ForwardIngress(
            self,
            self.get_interface(),
            ifip,
            cfg["bindport"])
        
        fi.set_init_host(host, port)

        log.info(f"--- Bootstrapping finished at {datetime.now().strftime('%H:%M:%S on %x')} ---")
        #self.runThreaded(fi.setHost)

        return {"forwardingress": fi, "balancer": self}
        # Ideally exit with 0 here, but GIL exists


    def run_threaded(self, job_func, args):
        """Runs a function in a new thread"""
        job_thread = threading.Thread(target = job_func, args = (args,))
        job_thread.start()

        
    def set_node_connection_timeout(self, timeout: int) -> bool:
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


    def get_node_connection_timeout(self) -> int:
        """Returns the connection/liveness reply timeout period in milliseconds"""
        return self.timeout


    def get_type(self) -> str:
        """Returns the mode of operation - primary or backup"""
        return self.type

    
    def get_mtu(self) -> int:
        """Returns the MTU (maximum transmission unit) of the current interface"""

        lif = LibIface(self.ifname)
        sysmtu = lif.get_mtu()

        if sysmtu != self.pmtu:
            log.warning("Interface MTU modified externally! Expected {} but found {}".format(self.pmtu, sysmtu))

        return sysmtu


    def set_mtu(self, mtu: int) -> bool:
        """Sets MTU (maximum transmission unit) for the current interface"""

        lif = LibIface(self.ifname)

        if self.no_mtu == False and self.static_mtu == False:
            # Set system MTU first
            newmtu = lif.set_mtu(mtu)
            # Set instance MTU
            self.pmtu = newmtu
        elif self.static_mtu == True:
            newmtu = lif.set_mtu(self.pmtu)
        else:
            return 2

        # Check and report
        if self.pmtu == lif.get_mtu():
            return True
        else:
            return False


    def set_interface(self, interface: str) -> None:
        """Sets the interface to use for operation"""
        self.ifname = interface


    def get_interface(self) -> str:
        """Returns the name of the current interface in use"""
        return self.ifname


    def set_selection_algorithm(self, algorithm: str) -> None:
        self.algorithm = algorithm


    def get_selection_algorithm(self) -> str:
        return self.algorithm

    
    def get_hostname(self) -> str:
        """
        Returns the node's hostname
        Note: This is the device name, not the LB node name
        """
        return socket.gethostname()


    def get_interface_ip(self, interface: str) -> str:
        """Returns the IP address of a specific interface"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        return socket.inet_ntoa(ioctl
                                (sock.fileno(),
                                SIOCGIFADDR, # This shouldn't be a privileged request
                                struct.pack(
                                    '256s',
                                    bytes(interface.encode("utf-8"))))[20:24]) # Struct takes char[]


    def set_node_name(self, name: str) -> None:
        """Sets the named node name for use in the network"""
        self.name = name


    def get_node_name(self) -> str | None:
        """Returns the named node name for use in the network"""
        return self.name


# Balancer -> Dest. Host -> Client
# REWORK USING ASYNCIO!
class ForwardEgress:


    def __init__(self, destination: str, destport: int, balancer: Balancer) -> None:
        self.balancer = balancer
        self.destination = destination
        self.destport = destport


    def __socket_options(self, sock: socket, idle = 1, maxretrcount = 5, interval = 3, lingersecs = 5) -> None:
        """Sets socket options, also configures the socket for keep-alive"""
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, idle)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, interval)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, maxretrcount)
        #sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, lingersecs)


    async def forward_traffic(self, data, writer, wait) -> bool:
        """Forwards the incoming traffic to a configured host using asyncio streams"""
        try:
            reader_remote, writer_remote = await asyncio.open_connection(self.destination, self.destport)
            # Apply socket options to the underlying socket if available
            sock = writer_remote.get_extra_info('socket')
            if sock:
                self.__socket_options(sock, 1, 5, 3, 5)

            if wait != False:
                writer_remote.write(data)
                await writer_remote.drain()
                data = b''
                while True:
                    chunk = await reader_remote.read(2048)
                    if not chunk:
                        break
                    data += chunk
            else:
                writer_remote.write(data)
                await writer_remote.drain()
                data = await reader_remote.read(2048)

            # Close remote writer
            try:
                writer_remote.close()
                await writer_remote.wait_closed()
            except Exception:
                pass

            size = Helpers().determine_payload_size(data)
            log.debug(f"Server reply payload packet size is {size} bytes")

            if size > self.balancer.get_mtu():
                if size < 5000:
                    log.warning("Packet size is greater than interface MTU! It will now be changed to prevent fragmentation")
                    # Run potentially blocking set_mtu in a thread to avoid blocking the loop
                    await asyncio.to_thread(self.balancer.set_mtu, size)
                else:
                    log.warning("Packet size is greater than interface MTU! It will not be changed as it exceeds the 5000 byte limit")
                    log.info("Fragmentation may now occur")

            # Send back to the client
            writer.write(data)
            await writer.drain()
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

            return None
        except Exception:
            raise # Reraise last exception


    @deprecation.deprecated(details = "Do not use")
    async def check_node_state(self) -> list | bool:
        slib = ServerLibrary(log)
        result = await slib.ping_host(self.destination, self.destport)

        if result == True:
            latencydata = slib.get_latency_to_host(self.destination)
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
        self.init_params_set = False
        self.exit_request = False


    def set_init_host(self, host: str, port: int) -> bool | None:
        """Sets initial params for the server to function - single use function"""
        if self.init_params_set == False:
            self.dest = host
            self.destport = int(port)
            self.init_params_set = True
        else:
            return False


    async def host_loop(self, repoll: int) -> None:
        """Periodic host selection loop (async). Set `exit_request` to True to stop."""
        # Initial small delay to stagger startup
        await asyncio.sleep(1)
        while not self.exit_request:
            # Set host
            if self.balancer.get_selection_algorithm() == "latency":
                log.info("Determining the best host")
                host = ServerLibrary(log).pick_latency_host()

                hostip, hostport = host["host"].split(':')

                self.dest = hostip
                self.destport = int(hostport)

                log.info(f"Selected \"{self.dest}:{self.destport}\" as the destination")
            elif self.balancer.get_selection_algorithm() == "roundrobin":
                lasthost = self.dest + ':' + str(self.destport)
                host = ServerLibrary(log).pick_round_robin_host(lasthost)

                hostip, hostport = host.split(':')

                self.dest = hostip
                self.destport = int(hostport)

                log.info(f"Selected \"{self.dest}:{self.destport}\" as the destination")

            # Wait for next poll or exit
            await asyncio.sleep(repoll)


    def get_loop(self):
        return self.loop


    def set_thread_exit(self):
        self.exit_request = True

    
    def __set_loop(self, loop):
        self.loop = loop
        

    async def ingress_server(self):
        # Use asyncio's reuse options rather than low-level socket flags
        server = await asyncio.start_server(self.response, self.bindip, self.port, family = socket.AF_INET, reuse_address=True)

        self.__set_loop(server.get_loop())

        async with server:
            await server.serve_forever()

    
    async def response(self, reader, writer): # NOTE: writer will write data back to client in this instance
        data = await reader.read(2048)

        size = Helpers().determine_payload_size(data)
        log.debug(f"Client payload packet size is {size} bytes")

        # This will replace the host address header
        try:
            # This will match the host and port
            hostRegex = "Host: (?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?):([1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])"
            bytestring = re.sub(hostRegex, f'Host: {self.dest}:{self.destport}', data.decode('utf-8'), count = 1)

            data = bytes(bytestring.encode('utf-8'))

            if size > self.balancer.get_mtu():
                if size < 5000:
                    log.warning("Packet size is greater than interface MTU! It will now be changed to prevent fragmentation")
                    self.balancer.set_mtu(size)
                else:
                    log.warning("Packet size is greater than interface MTU! It will not be changed as it exceeds the 5000 byte limit")
                    log.info("Fragmentation may now occur")

            traffic = await ForwardEgress(self.dest, self.destport, self.balancer).forward_traffic(data, writer, True)

            if traffic != None:
                log.error("The request failed!")
        except:
            log.warning("Invalid data received or the kernel refused to change system MTU - check logs")


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
                    balancer.set_interface(arg)
                elif opt in ("-m", "--staticmtu"):
                    balancer.set_mtu(int(arg))
                    balancer.use_static_mtu(True)
                elif opt in ("-n", "--nomtu"):
                    balancer.no_mtu_change(True)
                elif opt in ("-c", "--conf"):
                    cfg = helpers.ConfigParser(arg)
    except getopt.GetoptError:
        print("Unknown argument\nbalancer.py [hmicn] --staticmtu= --iface= --conf= --nomtu")
        sys.exit(1)

    async def _main_async():
        # Bootstrap synchronously (may perform short blocking ops)
        result = balancer.bootstrap(cfg)
        fi = result["forwardingress"]
        balancer_instance = result["balancer"]

        # Start host selection and heartbeat loops as asyncio tasks
        host_task = asyncio.create_task(fi.host_loop(cfg["repoll_time"]))
        hb_task = asyncio.create_task(balancer_instance.heartbeat_loop(cfg["heartbeat_time"]))
        server_task = asyncio.create_task(fi.ingress_server())

        try:
            await server_task
        except asyncio.CancelledError:
            pass
        finally:
            # Signal tasks to stop and cancel
            fi.set_thread_exit()
            host_task.cancel()
            hb_task.cancel()
            try:
                await asyncio.gather(host_task, hb_task, return_exceptions=True)
            except Exception:
                pass
            # Try to shutdown the server loop
            try:
                Helpers().shutdown(fi.get_loop())
            except Exception:
                pass

    try:
        asyncio.run(_main_async())
    except KeyboardInterrupt:
        log.info("Shutdown requested by user")
        # Note: asyncio.run already cancels tasks on KeyboardInterrupt
    
    log.info("Shutdown complete")