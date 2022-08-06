import getopt, sys
from balancerlibs.libnet import LibIface

class System:

    def cli(self, opts):
        print(opts)
        try:
            for opt, arg in opts:
                if opt == '-h':
                    print("balancer.py [hmni] --staticmtu= --iface= --nomtu")
                    sys.exit()
                elif opt in ("-i", "--iface"):
                    iface = arg
                elif opt in ("-m", "--staticmtu"):
                    li = LibIface(arg)
                elif opt in ("-n", "--nomtu"):
                    outputfile = arg
        except getopt.GetoptError:
            sys.exit(2)