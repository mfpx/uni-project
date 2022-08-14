import random
import sys
import time, os, asyncio, datetime, numpy, concurrent.futures, gc, ipaddress
from time import sleep
from ping3 import ping, EXCEPTIONS
from yaml import load, dump

# https://pyyaml.org/wiki/PyYAMLDocumentation
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

#gc.set_debug(gc.DEBUG_LEAK)

class Helpers:
    """Balancer system helper functions"""

    def ConfigParser(self, config = "config.yml") -> int | dict:
        try:
            configfile = open(config, 'r')
            configdict = load(configfile, Loader = Loader)
        except:
            raise OSError("Unable to open the configuration file")

        CONFIG_CONTENTS = {
         "hostname": str,
         "node_type": str,
         "bindip": str,
         "multicast_group": str,
         "multicast_port": int,
         "network_password": str,
         "thread_count": int,
         "bindport": int,
         "default_pmtu": int,
         "ifname": str,
         "timeout": int,
         "repoll_time": int,
         "algorithm": str,
         "servers": list}

        for key in CONFIG_CONTENTS:
            if key not in configdict.keys():
                raise KeyError(f"A required parameter {key} not found in the configuration file")

        for key in configdict:
            if not isinstance(configdict[key], CONFIG_CONTENTS[key]):
                raise

        for ipaddr in configdict['servers']:
            ip, port = ipaddr.split(':')
            try:
                ipaddress.ip_address(ip)
            except ValueError:
                print(f"\"{ip}\" is not an IP, it will be parsed as a DNS name!")

        return configdict


    def generateNodeName(self) -> str:
        nouns = open("nouns.txt", "r")
        adjectives = open("adjectives.txt", "r")

        noun = self.__random_line(nouns).rstrip("\r\n").lower()
        adjective = self.__random_line(adjectives).rstrip("\r\n").lower()

        name = (adjective + " " + noun).replace(" ", "-")

        return name

    # Source: https://stackoverflow.com/questions/3540288/how-do-i-read-a-random-line-from-one-file
    def __random_line(self, afile):
        line = next(afile)
        for num, aline in enumerate(afile, 2):
            if random.randrange(num):
                continue
            line = aline
        return line


    def determinePayloadSize(self, payload):
        # Object must be bytes
        if isinstance(payload, bytes):
            return len(payload)
        else:
            return -1


    def shutdown(self, loop: asyncio.BaseEventLoop) -> bool:
        """Stops the EventLoop and waits for it to close"""
        loop.stop()
        if loop.is_closed():
            return True
        else:
            return False
            

class ServerLibrary:

    def __init__(self, log) -> None:
        self.log = log


    async def PingHost(self, host, port, duration = 10, delay = 2) -> bool:
        """Repeatedly try if a port on a host is open until duration seconds passed
        
        Parameters
        ----------
        host : str
            host ip address or hostname
        port : int
            port number
        duration : int, optional
            Total duration in seconds to wait, by default 10
        delay : int, optional
            delay in seconds between each try, by default 2
        
        Returns
        -------
        awaitable bool
        """
        tmax = time.time() + duration
        while time.time() < tmax:
            try:
                _reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=5)
                writer.close()
                await writer.wait_closed()
                print("Host is up and the port is accessible")
                return True
            except:
                if delay:
                    await asyncio.sleep(delay)
        print("Host is down or not responding")
        return False


    # Rework the selection - maybe use avg latency tested every n seconds?
    def __calculateNewScore(self, avglatency, score) -> float:
        if avglatency > 0.2:
            pts = avglatency / 10 # This should result in a low value
            if pts >= 1.0:
                return round(score - pts, 6)
            else:
                return 10 # False positive, return 10 as default
        else:
            return round(score - avglatency, 6)


    def getLatencyToMultipleHosts(self, hosts, interval = 1, threshold = 0.05, repetitions = 5) -> list:
        results = []

        if type(hosts) != list:
            raise TypeError(f"Function expected a list, but got {type(hosts)}")
        else:
            for host_array in hosts:
                item = self.getLatencyToHost(host_array[0][0], host_array[1], interval, threshold, repetitions)
                item["host"] = host_array[0][0] + ":" + host_array[0][1]
                results.append(item)

        return results

        
    # Sub-second threshold values might cause a time out due to request throttling
    def getLatencyToHost(self, host, ttl = 64, interval = 1, threshold = 0.05, repetitions = 5) -> dict:

        # Source: https://github.com/FlipperPA/latency-tester/blob/main/latency-tester.py

        count = 0
        avglatency = 0
        items = {"host": "localhost", "latency": 0.0, "score": 10} # Function reference and template for dict ordering

        header = f"Pinging {host} every {interval} secs; threshold: {threshold} secs."
        print(header)

        while count < repetitions: # Attempt n times
            count += 1
            try:
                latency = ping(host, ttl = ttl)
            except PermissionError:
                print("Critical: ICMP-flagged sockets cannot be created by unprivileged users in your system")
                sys.exit(1)

            if latency != None:
                avglatency = (avglatency + latency) / repetitions
            else:
                self.log.warning("Request timed out")
                break

            if latency is None:
                latency_text = "PACKET DROPPED"
            else:
                latency_text = f"{round(latency, 4)} secs"

            line = f"{datetime.datetime.now()}: Sent ICMP packet to {host}; latency is {latency_text}"
            self.log.debug(line)
            if latency > threshold:
                score = self.__calculateNewScore(avglatency, items["score"])
                items["score"] = score
                self.log.info(f"Latency is high, selection score is now {score}")
                
            items["latency"] = round(avglatency, 4)
            items["host"] = host
            sleep(interval)
        self.log.debug(f"Average latency was {items['latency']}")

        return items

    
    def pickRoundRobinHost(self, lastHost: str) -> str:
        # Get all hosts
        config = Helpers().ConfigParser()
        hostlist = config["servers"]

        hosts = {}
        # Assign an index to each host
        for idx, host in enumerate(hostlist):
            hosts[idx] = host

        # Find the index of the host - this is expensive
        lastHostIndex = list(hosts.keys())[list(hosts.values()).index(lastHost)]

        # Does the next index value make sense?
        if lastHostIndex + 1 >= len(hosts):
            return hosts[0] # If not, start from 0
        else:
            return hosts[lastHostIndex + 1] # If it does, return the value


    def pickLatencyHost(self, ttl = 64) -> dict:
        # First get all hosts
        config = Helpers().ConfigParser()
        hostlist = config["servers"]

        # Second, add TTL to each host
        host_ttl = []
        for host in hostlist:
            ip, port = host.split(':')
            host_ttl.append([[ip, port], ttl])

        # Great, now we need to test their latency
        latencies = self.getLatencyToMultipleHosts(host_ttl)
       
        # Get only latencies
        latency = []
        for item in latencies:
            latency.append(item["latency"])

        # Append all scores to the list
        score = []
        for item in latencies:
            score.append(item["score"])

        # Get min latency and max score
        lowest_latency = min(latency)
        highest_score = max(score)

        for item in latencies:
            if highest_score >= 9.8: # It appears that the score doesn't normally go lower than 9.8
                if lowest_latency == item["latency"]:
                    return item
            else:
                if highest_score == item["score"]:
                    return item