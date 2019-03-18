# CHORD (DHT)

This repository implement a simple version of the [CHORD](https://en.wikipedia.org/wiki/Chord_(peer-to-peer)) algorithm.
The provided code already setups the ring network properly.
1. Supports Node Joins
2. Finds the correct successor for a node
3. Run Stabilize periodically to correct the network


## Running the example
Run in two different terminal:

DHT (setups a CHORD DHT):
```console
$ python3 DHT.py
```
example (put and get objects from the DHT):
```console
$ python3 example.py
```

## Authors

* **Mário Antunes** - [mariolpantunes](https://github.com/mariolpantunes)

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details