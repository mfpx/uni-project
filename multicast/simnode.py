from mcast import *

mcast = MulticastRx((("224.3.29.71", 10000)))
mcast.set_sock_timeout(60)
sock = mcast.create_socket()
while True:
    data = mcast.listen(sock)

    crypt = CryptoLib("password")
    data = crypt.decrypt_json(data['data'])

    print(data)