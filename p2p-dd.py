#! /usr/bin/env python3

import argparse
import logging
import socket
import ipaddress
import threading
import pickle
import hashlib

def logger(f):
    def inner(*args, **kwargs):
        name = f'{f.__qualname__}'
        return f(*args, **kwargs, log=logging.getLogger(name))
    return inner

@logger
def threaded(f, log):
    def inner(*args, **kwargs):
        t = threading.Thread(target=f, args=args, kwargs=kwargs)
        log.debug(f'starting thread {t}')
        t.start()
    return inner

def hash_data(data):
    m = hashlib.md5()
    if not type(data) is bytes:
        m.update(pickle.dumps(data))
    else:
        m.update(data)
    return m.hexdigest()

class SETTINGS:
    __slots__ = ()

    MESSAGE_SIZE_BYTES = 8
    BYTE_ORDER = 'big'
    RECEIVE_BUFFER_SIZE = 1024 # in bytes

SETTINGS = SETTINGS()

class MSG_TYPE:
    __slots__ = ()

    HANDSHAKE = 0
    UPDATE_PEERS = 1

MSG_TYPE = MSG_TYPE()

# from https://stackoverflow.com/a/28950776
def get_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # doesn't even have to be reachable
        s.connect(('10.255.255.255', 1))
        IP = s.getsockname()[0]
    except:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP

class ClientSocket:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port

    @logger
    def __enter__(self, log):
        log.debug(f'initiating connection with {self.ip}:{self.port}')
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.ip, self.port))
        return self

    @logger
    def __exit__(self, *args, log):
        log.debug(f'closing connection with {self.ip}:{self.port}')
        self.sock.close()

    @logger
    def send(self, data, log):
        msg_size = len(data).to_bytes(SETTINGS.MESSAGE_SIZE_BYTES,
                SETTINGS.BYTE_ORDER)
        log.debug(f'sending data to {self.ip}:{self.port} of size {len(data)}')
        self.sock.sendall(msg_size + data)

class ServerSocket:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port

    @logger
    def __enter__(self, log):
        log.debug(f'starting server socket on {self.ip}:{self.port}')
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((self.ip, self.port))
        self.sock.listen()
        return self

    @logger
    def __exit__(self, *args, log):
        log.debug(f'closing server socket on {self.ip}:{self.port}')
        self.sock.close()

    @logger
    def accept(self, log):
        csock, addr = self.sock.accept()
        log.debug(f'accepted connection from {addr[0]}:{addr[1]}')
        return ReceiveSocket(csock)

class ReceiveSocket:
    def __init__(self, csock):
        self.csock = csock

    def __del__(self):
        self.csock.close()

    @logger
    def read(self, log):
        remote_ip = self.csock.getpeername()[0]
        log.debug(f'trying to read message from {remote_ip}')

        def get(size):
            cp = 0
            chunks = bytearray()
            while cp < size:
                chunk = self.csock.recv(min(SETTINGS.RECEIVE_BUFFER_SIZE, size))
                if not chunk:
                    break
                cp += len(chunk)
                chunks += chunk
            if not chunks:
                raise RuntimeError('Connection error')
            return bytes(chunks)

        size = int.from_bytes(get(SETTINGS.MESSAGE_SIZE_BYTES),
                SETTINGS.BYTE_ORDER)

        log.debug(f'message from {remote_ip} is {size} bytes long')
        data = get(size)
        log.debug(f'finished reading message from {remote_ip}')
        return data

    def remote_ip(self):
        return self.csock.getpeername()[0]

def encoder(msg_type, payload):
    return pickle.dumps({
        'type': msg_type,
        'payload': payload
        })

def decoder(data):
    data = pickle.loads(data)
    return data['type'], data['payload']

@logger
def post(remote, msg_type, payload, log):
    log.debug(f'posting message of type {msg_type} to {remote}')

    with ClientSocket(remote, cmd_args.port) as sock:
        sock.send(encoder(msg_type, payload))

class Server:
    class Peer:
        def __init__(self, ip, seeded):
            self.ip = ip
            self.seeded = seeded

        def __eq__(self, other):
            return self.ip == other.ip

        def __hash__(self):
            return hash(self.ip)

        def __str__(self):
            return f'{self.ip} (seeded: {self.seeded})'

    @logger
    def __init__(self, port, seeded, log):
        self.ip = get_ip()
        self.seeded = seeded
        self.peers = set([Server.Peer(self.ip, self.seeded)])
        self.port = port

        self.peer_lock = threading.Lock()

        self.listen_loop()

    @threaded
    @logger
    def listen_loop(self, log):
        log.info(f'starting server on {self.ip}:{self.port}')

        with ServerSocket(self.ip, self.port) as sock:
            log.debug(f'ready to accept connections on {self.ip}:{self.port}')
            while True:
                csock = sock.accept()
                self.handler(csock)

    @threaded
    @logger
    def handler(self, csock, log):
        msg_type, payload = decoder(csock.read())
        src = csock.remote_ip()
        log.info(f'received message of type {msg_type}')

        if msg_type == MSG_TYPE.HANDSHAKE:
            self.add_peer(src, payload)
        elif msg_type == MSG_TYPE.UPDATE_PEERS:
            self.update_peers(src, payload)

    @logger
    def add_peer(self, ip, seeded, log):
        with self.peer_lock:
            peer = Server.Peer(ip, seeded)
            if not peer in self.peers:
                log.warning(f'adding peer {peer} to network')
                self.peers.add(peer)
            post(ip, MSG_TYPE.UPDATE_PEERS, self.peers)

    @logger
    def update_peers(self, src, ngh_peers, log):
        with self.peer_lock:
            new_peers = ngh_peers - self.peers
            if new_peers:
                log.warning(f'received {len(new_peers)} from neighbour {src}')
                self.peers = self.peers.union(new_peers)

            new_peers = self.peers - ngh_peers
            if new_peers:
                log.info(f'sending peers back to {src}')
                post(src, MSG_TYPE.UPDATE_PEERS, new_peers)

class PeeringServer:
    def __init__(self, server, remote_port, network_prefix):
        self.remote_port = remote_port
        self.server = server
        self.network_prefix = network_prefix
        self.find_peers()

    @threaded
    @logger
    def find_peers(self, log):
        net = ipaddress.ip_network(f'{get_ip()}/{self.network_prefix}',
                strict=False)
        log.info(f'starting peer discovery on {net}')

        own_ip = get_ip()
        for host in map(str, net.hosts()):
            if host != own_ip:
                self.ping_ip(host)

    @threaded
    @logger
    def ping_ip(self, ip, log):
        log.debug(f'trying to peer with {ip}')
        try:
            post(ip, MSG_TYPE.HANDSHAKE, self.server.seeded)
            log.info(f'handshake sent to {ip}')
        except:
            log.debug(f'unable to connect to {ip}')

@logger
def main(log):
    parser = argparse.ArgumentParser()

    parser.add_argument('file', help='the file to mirror')

    def prefix_type(a):
        a = int(a)
        if not 0 <= a <= 32:
            raise argparse.ArgumentTypeError('network prefix must be between'
                    ' 0 and 32')
        return a

    def port_type(a):
        a = int(a)
        if not 1 <= a <= 65535:
            raise argparse.ArgumentTypeError('network port must be between'
                    ' 1 and 65535')
        return a

    parser.add_argument('--prefix', type=prefix_type, default=24,
            help='the network prefix')
    parser.add_argument('--port', type=port_type, default=8500,
            help='the port on which to listen')

    parser.add_argument('--batch', action='store_true',
            help='no user interaction, assumes this node is not pre-seeded')
    parser.add_argument('--seeded', action='store_true',
            help='set this node as pre-seeded')
    parser.add_argument('--verbose', '-v', action='count', default=0,
            help='verbosity level, repeat to increase')

    global cmd_args
    cmd_args = parser.parse_args()

    logging.basicConfig(level=max(10, 30-(cmd_args.verbose * 10)))
    log.debug(f'called with arguments {vars(cmd_args)}')

    if cmd_args.batch or cmd_args.seeded:
        seeded = cmd_args.seeded
    else:
        while True:
            ans = input('Is this node pre-seeded? [Y/n] ') or 'Y'
            if not ans.lower() in ['y', 'yes', 'n', 'no']:
                print('Invalid input, please try again')
                continue
            ans = True if ans.lower() in ['y', 'yes'] else False
            break
        seeded = ans

    server = Server(cmd_args.port, seeded)
    peer_finder = PeeringServer(server, cmd_args.port, cmd_args.prefix)

if __name__ == '__main__':
    main()
