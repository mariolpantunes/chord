# coding: utf-8

import socket
import pickle
import logging


class DHT_Client():
    def __init__(self, address):
        self.dht_addr = address
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.logger = logging.getLogger('DHT_Client')

    def put(self, key, value):
        msg = {'method': 'PUT', 'args':{'key':key, 'value': value}}
        p = pickle.dumps(msg)
        self.socket.sendto(p, self.dht_addr)
        p, addr = self.socket.recvfrom(1024)
        o = pickle.loads(p)
        if o['method'] != 'ACK':
            self.logger.error('Invalid msg: %s', o)

    def get(self, key):
        msg = {'method': 'GET', 'args': {'key': key}}
        p = pickle.dumps(msg)
        self.socket.sendto(p, self.dht_addr)
        p, addr = self.socket.recvfrom(1024)
        o = pickle.loads(p)
        if o['method'] != 'ACK':
            self.logger.error('Invalid msg: %s', o)
            return None
        else:
            return o['args']
