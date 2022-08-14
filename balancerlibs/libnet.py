import re
import socket
import struct
import logging
import subprocess
from fcntl import ioctl
import psutil

# Do not edit unless you know what you are doing
# Messing with system calls could crash things or introduce instabilities
# ESPECIALLY when running things as a privileged user - as this is meant to be run

"""
Get or set the MTU (Maximum Transfer Unit) of a device
using ifr_mtu.  Setting the MTU is a privileged operation.
Setting the MTU to too small values may cause kernel
crashes.

Source: netdevice.7 manpage
"""
SIOCGIFMTU = 0x8921 # DO NOT CHANGE
SIOCSIFMTU = 0x8922 # DO NOT CHANGE

log = logging.getLogger(__name__)

try:
    import _capng as capng # AUR python-capng, libcap-ng and libcap
    CAPNG_EXISTS = True
except ModuleNotFoundError:
    CAPNG_EXISTS = False
    log.warning("No capng bindings found for the current Python instance, POSIX capabilities will not be available!")
except:
    CAPNG_EXISTS = False
    
# Source: https://archlinux.org/packages/?sort=&q=libcap&maintainer=&flagged=
# CapNg docs: https://people.redhat.com/sgrubb/libcap-ng/python.html


class LibIface:


    def __init__(self, ifname: str):
        self.ifname = ifname

    
    def __call__(self) -> None:
        print("MTU (Maximum Transmission Unit) management tool\nDo not call the class directly, instantiate it first!")


    def get_address_mtu(self, ip: str) -> int:
        """
        Returns MTU for a specified IP address
        Using REGEXP (P is the additional features from Perl) and iproute2
        """

        routeinfo = subprocess.check_output(['ip', 'route', 'get', ip])
        dev = re.search('.*dev (\w+) .*', routeinfo).groups()[0]
        mtuinfo = subprocess.check_output(['ip', 'link', 'show', dev])
        mtu = re.search('.*mtu ([0-9]+) .*', mtuinfo).groups()[0]

        return int(mtu)


    def get_mtu(self) -> int:
        """
        Retrieves MTU using socket ioctl syscall
        """

        sock = socket.socket(type = socket.SOCK_DGRAM) # Set to UDP
        ifr = self.ifname + '\x00'*(32-len(self.ifname))
        try:
            ifs = ioctl(sock, SIOCGIFMTU, ifr)
            mtu = struct.unpack('<H',ifs[16:18])[0]
        except Exception as s:
            log.critical('Socket ioctl call failed: {0}'.format(s))
            raise

        log.debug('getMTU: mtu of {0} = {1}'.format(self.ifname, mtu))
        self.mtu = mtu
        return mtu


    def set_mtu(self, mtu: int) -> int:
        """
        Uses socket ioctl syscall to set the MTU
        """
        sock = socket.socket(type = socket.SOCK_DGRAM) # Set UDP
        # Little endian, char[] in bytes and its an unsigned short
        ifr = struct.pack('<16sH', bytes(self.ifname.encode('utf-8')), mtu) + bytes('\x00'.encode('utf-8')*14)
        try:
            ifs = ioctl(sock, SIOCSIFMTU, ifr)
            # Little endian and a 2 byte unsigned short
            self.mtu = struct.unpack('<H',ifs[16:18])[0]
        except Exception as s:
            log.critical('Socket ioctl call failed: {0}'.format(s))
            raise

        log.debug('setMTU: mtu of {0} = {1}'.format(self.ifname, self.mtu))

        return self.get_mtu()


    def get_available_ifaces(self, ifa = 0) -> list:
        """
        Returns available network interfaces on the system and their information
        """

        ifaces = [] # Hard ref to prevent untimely garbage collection

        for a, b in psutil.net_if_addrs().items():
            log.debug("Interface {} has address {}".format(a,b[0].address))
            ifaces.append([a, b])

        if ifa != 0:
            for i in ifaces:
                if i[0] == ifa:
                    return [i[0], i[1][0].address]

        return ifaces


if __name__ == "__main__":
    import sys, os, platform
    logging.basicConfig(filename="libnet.log", level=logging.DEBUG)

    if platform.system() != 'Linux':
        log.warning("Only tested on Linux, may not work on other systems!")

    if CAPNG_EXISTS == True:
        if os.geteuid() != 0 and capng.capng_have_capabilities(capng.CAP_NET_ADMIN) != 1:
            # On Linux, user capabilities are stored in /etc/security/capability.conf
            # More often than not, this can be easiest achieved with libcap which provides the pam_cap.so PAM module
            # Install libcap, edit capability.conf, add "auth required pam_cap.so" to /etc/pam.d/login, restart
            # NOTE: You MAY or MAY NOT need to setcap on the python executable to inherit user perms
            sys.exit("You must run this tool as root, or have the CAP_NET_ADMIN privilege (see capabilities.7 manpage), exiting")
        else:
            print("MTU (Maximum Transmission Unit) management tool")
    else:
        if os.geteuid() != 0:
            sys.exit("You must run this tool as root")
        else:
            print("MTU (Maximum Transmission Unit) management tool")


    mtu = None
    if len(sys.argv) > 2:
        dev, mtu = sys.argv[1:]
    elif len(sys.argv) > 1:
        dev = sys.argv[1]
    else:
        dev = 'eth0'

    iface = LibIface(dev)
    if mtu != None:
        iface.set_mtu(int(mtu))

    print (f"Interface \"{dev}\" has an MTU of {iface.get_mtu()}")
