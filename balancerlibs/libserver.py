import time, os, asyncio, datetime, numpy, concurrent.futures, gc, ipaddress
from time import sleep
from ping3 import ping
from yaml import load, dump

# https://pyyaml.org/wiki/PyYAMLDocumentation
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

#gc.set_debug(gc.DEBUG_LEAK)

class Helpers:

    def ConfigParser(self, config = "config.yml") -> int | dict:
        configfile = open(config, 'r')
        configdict = load(configfile, Loader = Loader)

        CONFIG_CONTENTS = {"hostname": str, "servers": list}

        for key in configdict:
            if not isinstance(configdict[key], CONFIG_CONTENTS[key]):
                return -1

        for ipaddr in configdict['servers']:
            try:
                ipaddress.ip_address(ipaddr)
            except ValueError:
                print(f"\"{ipaddr}\" is not an IP, it will be parsed as a DNS name!")

        return configdict


    def determinePayloadSize(self, payload):
        # Object must be bytes
        if isinstance(payload, bytes):
            return len(payload)
        else:
            return -1
            

class ServerLibrary:


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
                print("host is up and the port is accessible")
                return True
            except:
                if delay:
                    await asyncio.sleep(delay)
        print("host is down or not responding")
        return False


    # Rework the selection - maybe use avg latency tested every n seconds?
    def __calculateNewScore(self, avglatency, score) -> float:
        if avglatency > 0.2:
            pts = avglatency / 10 # This should result in a low value
            print(f"pts is {pts}")
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
                item = self.getLatencyToHost(host_array[0], host_array[1], interval, threshold, repetitions)
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

        while count < repetitions: # Attempt 5 times
            count += 1
            latency = ping(host, ttl = ttl)

            if latency != None:
                avglatency = (avglatency + latency) / repetitions
            else:
                print("Request timed out", flush=True)
                break

            if latency is None:
                latency_text = "PACKET DROPPED"
            else:
                latency_text = f"{round(latency, 4)} secs"

            line = f"{datetime.datetime.now()}: Sent ICMP packet to {host}; latency is {latency_text}"
            print(f"{line}")
            if latency > threshold:
                score = self.__calculateNewScore(avglatency, items["score"])
                items["score"] = score
                print(f"latency is high, selection score is now {score}")
                
            items["latency"] = avglatency
            items["host"] = host
            sleep(interval)

        return items


    def pickLatencyHost(self, ttl = 64):
        # First get all hosts
        config = Helpers().ConfigParser()
        hostlist = config['servers']

        # Second, add TTL to each host
        host_ttl = []
        for host in hostlist:
            host_ttl.append([host, ttl])

        # Great, now we need to test their latency
        latencies = self.getLatencyToMultipleHosts(host_ttl)
       
        # Get only latencies
        latency = []
        for item in latencies:
            latency.append(item['latency'])

        # Append all scores to the list
        score = []
        for item in latencies:
            score.append(item['score'])

        # Get min latency and max score
        lowest_latency = min(latency)
        highest_score = max(score)

        for item in latencies:
            if highest_score >= 9.8: # It appears that the score doesn't normally go lower than 9.8
                if lowest_latency == item['latency']:
                    return item
            else:
                if highest_score == item['score']:
                    return item


# --- DEBUG CODE --- DO NOT USE IN PRODUCTION ---

#slib = ServerLibrary()
#print(slib.getLatencyToMultipleHosts([["google.com", 64], ["qmul.ac.uk", 64], ["stumbra.co.uk", 64]], 0.5))

hel = Helpers()
hel.ConfigParser()
slib = ServerLibrary()
slib.pickLatencyHost()