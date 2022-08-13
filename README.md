# uni-project
MSc University Project

This is my (David Stumbra's) University project for the MSc degree at Queen Mary, University of London.

## Setup
1. Install the necessary dependencies, namely python-capng, libcap-ng and libcap if you intended to utilise POSIX capabilities
2. Install Python 3.10.* and necessary libraries (ping3, pyyaml, pycryptodome and schedule)
3. Configure the server and downstream hosts (config.yml)
4. Run balancer.py (if you're using POSIX capabilities, make sure you have CAP_NET_ADMIN capability, otherwise run as root)

## Notes
This was tested on Linux kernel versions 5.15.49 and 5.18.0.
It might work on older versions, BUT... It might also work on OSX - give it a try!
This software was not made to work on Windows, and will almost certainly not even start