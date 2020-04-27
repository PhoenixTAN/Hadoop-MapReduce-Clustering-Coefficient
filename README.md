# Clustering-Coefficient-Hadoop-MapReduce
Use MapReduce technique in Hadoop Java to count triangles in an undirected graph to calculate the clustering coefficient.

## How to run?

### Dependency
1. Hadoop 3.2.1.
2. Ubuntu 16.04 LTS in windows 10.
3. Openjdk version 1.8.0_252 in Ubuntu.

Terminal in Ubuntu:
```
$ /bin/hadoop jar TriangleCounterNaive.jar NaiveTriangleCounter [Argument]
```
where [Argument] is a required argument:
- 1: a tiny test graph (/dataset/test/test.txt).
- 2: a small graph (/dataset/small-graph/smallgraph.txt).
- 3: a big graph (/dataset/big-graph/graph.txt).

## Clustering Coefficient and Triangle Counting
Paper is here.
https://theory.stanford.edu/~sergei/papers/www11-triangles.pdf

The clustering coefficient measures the degree to which a node’s neighbors are themselves neighbors. In sociology, it describes how tight the community of someone is.

![alt text](./README-IMAGES/cc.png)

The numerator in the above quotient is the number of edges between neighbors of v. 

The denominator is the number of possible edges between neighbors of v. 

There is also an equivalent way to view the clustering coefficient. 

{u,v,w} must form a triangle.

![alt text](./README-IMAGES/tight-community.png)

## Naive Algorithm
### Single process version
![alt text](./README-IMAGES/algorithm1.png)

### MapReduce version (parallel)

## An improved Algorithm
![alt text](./README-IMAGES/algorithm3.png)

