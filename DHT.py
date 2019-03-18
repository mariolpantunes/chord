# coding: utf-8

import logging
import time
from DHT_Node import DHT_Node


# configure the log with DEBUG level
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M:%S')


def main(number_nodes):
    # logger for the main
    logger = logging.getLogger('DHT')
    # list with all the nodes
    dht = []
    # initial node on DHT
    node = DHT_Node(('localhost', 5000))
    node.start()
    dht.append(node)
    logger.info(node)

    for i in range(number_nodes-1):
        node = DHT_Node(('localhost', 5001+i), ('localhost', 5000))
        node.start()
        dht.append(node)
        logger.info(node)

    # Await for DHT to get stable
    time.sleep(10)

    for node in dht:
        node.join()


if __name__ == '__main__':
    main(5)
