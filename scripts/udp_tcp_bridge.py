#!/usr/bin/env python3
"""UDP<->TCP bridge with 2-byte length framing (QUIC datagrams over an ssh tunnel).
  serve  : listen TCP 127.0.0.1:TCP_PORT; for each connection open one UDP socket towards
           TARGET_HOST:TARGET_PORT and relay datagrams both ways (runs next to the server).
  client : listen UDP 127.0.0.1:UDP_PORT; relay datagrams to TCP 127.0.0.1:TCP_PORT and back
           (runs next to the client; the client dials 127.0.0.1:UDP_PORT)."""
import socket, struct, sys, threading, time

def tcp_read(sock):
    buf = b""
    while True:
        while len(buf) < 2:
            d = sock.recv(65536)
            if not d: return
            buf += d
        n = struct.unpack("!H", buf[:2])[0]
        while len(buf) < 2 + n:
            d = sock.recv(65536)
            if not d: return
            buf += d
        yield buf[2:2 + n]
        buf = buf[2 + n:]

def serve(tcp_port, host, port):
    ls = socket.socket(); ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    ls.bind(("127.0.0.1", tcp_port)); ls.listen(4)
    while True:
        c, _ = ls.accept(); c.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        u = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); u.connect((host, port))
        def up():
            for d in tcp_read(c): u.send(d)
        def down():
            while True:
                try: d = u.recv(65536)
                except OSError: return
                try: c.sendall(struct.pack("!H", len(d)) + d)
                except OSError: return
        threading.Thread(target=up, daemon=True).start()
        threading.Thread(target=down, daemon=True).start()

def client(udp_port, tcp_port):
    u = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); u.bind(("127.0.0.1", udp_port))
    while True:
        c = None
        while c is None:
            try:
                c = socket.create_connection(("127.0.0.1", tcp_port)); c.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError: time.sleep(1)
        peer = [None]
        def up():
            while True:
                try: d, a = u.recvfrom(65536)
                except OSError: return
                peer[0] = a
                try: c.sendall(struct.pack("!H", len(d)) + d)
                except OSError: return
        t = threading.Thread(target=up, daemon=True); t.start()
        for d in tcp_read(c):
            if peer[0]: u.sendto(d, peer[0])
        c.close(); time.sleep(1)

if __name__ == "__main__":
    if sys.argv[1] == "serve": serve(int(sys.argv[2]), sys.argv[3], int(sys.argv[4]))
    else: client(int(sys.argv[2]), int(sys.argv[3]))
