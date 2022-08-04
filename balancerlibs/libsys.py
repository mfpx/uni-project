import getopt, sys
from balancerlibs.libnet import LibIface

class System:

    def cli(self, opt):
        try:
            opts, args = getopt.getopt(argv,"hm:ni:", ["staticmtu=","iface=","nomtu"])
        except getopt.GetoptError:
            print("balancer.py [hmni] --staticmtu= --iface= --nomtu")
            sys.exit(2)
        for opt, arg in opts:
            if opt == '-h':
                print("balancer.py [hmni] --staticmtu= --iface= --nomtu")
                sys.exit()
            elif opt in ("-i", "--iface"):
                iface = arg
            elif opt in ("-m", "--staticmtu"):
                li = LibIface(ifname)
            elif opt in ("-n", "--nomtu"):
                outputfile = arg