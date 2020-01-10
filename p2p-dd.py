#! /usr/bin/env python3

import argparse
import logging
import socket
import ipaddress
import threading

def logger(f):
    def inner(*args, **kwargs):
        name = f'{__name__}.{f.__qualname__}'
        return f(*args, **kwargs, log=logging.getLogger(name))
    return inner

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

class PeerHandshake(threading.Thread):
    def __init__(self, csock):
        self.csock = csock

        super().__init__()

    @logger
    def run(self, log):
        log.debug(f'starting handshake with {self.csock}')

class PeeringServer(threading.Thread):
    def __init__(self, listen_ip, listen_port):
        self.ip = str(listen_ip)
        self.port = listen_port

        super().__init__()

    @logger
    def run(self, log):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind((self.ip, self.port))
            sock.listen()

            log.info(f'peering server started on {sock.getsockname()}')

            while True:
                csock, addr = sock.accept()
                log.debug(f'incoming connection from {addr}')
                PeerHandshake(csock).start()

class Peer:
    @logger
    def __init__(self, seeded, prefix, port, log):
        self.ip = ipaddress.ip_address(get_ip())
        self.port = port
        self.network = ipaddress.ip_network(f'{self.ip}/{prefix}', strict=False)
        self.peer_id = str(self.ip)

        log.info(f'peer created with peer-id {self.peer_id}')
        self.start_peering()

    @logger
    def start_peering(self, log):
        log.info(f'starting peer discovery on network {self.network}')

        self.start_peer_server()

    @logger
    def start_peer_server(self, log):
        log.info(f'starting peer server on {self.ip}')
        self.peering_server = PeeringServer(self.ip, self.port)
        self.peering_server.start()

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
            help='the peer discovery server port')

    parser.add_argument('--batch', action='store_true',
            help='no user interaction, assumes this node is not pre-seeded')
    parser.add_argument('--seeded', action='store_true',
            help='set this node as pre-seeded')
    parser.add_argument('--verbose', '-v', action='count', default=0,
            help='verbosity level, repeat to increase')

    args = parser.parse_args()

    logging.basicConfig(level=max(10, 30-(args.verbose * 10)))
    log.debug(f'called with arguments {vars(args)}')

    if args.batch or args.seeded:
        seeded = args.seeded
    else:
        while True:
            ans = input('Is this node pre-seeded? [Y/n] ') or 'Y'
            if not ans.lower() in ['y', 'yes', 'n', 'no']:
                print('Invalid input, please try again')
                continue
            ans = True if ans.lower() in ['y', 'yes'] else False
            break
        seeded = ans

    peer = Peer(seeded=seeded, prefix=args.prefix, port=args.port)

if __name__ == '__main__':
    main()
