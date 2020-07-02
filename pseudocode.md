# Workgroup Activity

## Table of contents

1. [The basic idea](#1-the-basic-idea)
2. [The Algorithm](#2-the-algorithm)
3. [Implementation](#3-implementation)

## 1. The basic Idea

K-Means is one of the most popular "clustering" algorithms. K-means stores _k_ centroids that it uses to define clusters. A point is considered to be in a particular cluster if it is closer to that cluster's centroid than any other centroid.

K-Means finds the best centroids by alternating between (1) assigning data points to clusters based on the current centroids (2) chosing centroids (points which are the center of a cluster) based on the current assignment of data points to clusters.

## 2. The Algorithm

We select Lloyd algorithm (1957, published 1982) is batch (also called offline) centroid models. A centroid is the geometric center of a convex object and can be thought of as a generalisation of the mean. Batch algorithms are algorithms where a transformative step is applied to all cases at once. It is well suited to analyse large data sets, since the incremental _k_-means algorithms require to store the cluster membership of each case or to do two nearest-cluster computations as each case is processed, which is computationally expensive on large datasets.

Lloyd algorithm considers the data distribution discrete. For a set of cases , where is the data space of _d_ dimensions, the algorithm tries to find a set of _k_ cluster centers that is a solution to the minimization problem: discrete distribution, where is the probability density function and _d_ is the distance function. Note here that if the probability density function is not known, it has to be deduced from the data available.

The first step of the algorithm is to choose the _k_ initial centroids. It can be done by assigning them based on previous _empirical_ knowledge, if it is available, by using _k_ random observations from the data set, by using the _k_ observations that are the farthest from one another in the data space or just by giving them random values within.

Once the _initial centroids_ have been chosen, iterations are done on the following two steps.

- In the first one, each case of the data set is assigned to a cluster based on its distance from the clusters centroids, using one of the metric previously presented.

- The second step is to update the value of the centroid using the mean of the cases assigned to the centroid. Those iterations are repeated until the centroids stop changing, within a tolerance criterion decided by the researcher, or until no case changes cluster.

The _k_-means clustering technique can be seen as partitioning the space into Voronoi cells (Voronoi, 1907). For each two centroids, there is a line that connects them. Perpendicular to this line, there is a line, plane or hyperplane (depending on the dimensionality) that passes through the middle point of the connecting line and divides the space into two separate subspaces. The _k_-means clustering therefore partitions the space into _k_ subspaces for which is the nearest centroid for all included elements of the subspace (Faber, 1994).

## 3. Implementation

Here is the pseudocode describing the iterations:

1. Choose the number of clusters
2. Choose the metric to use
3. Choose the method to pick initial centroids
4. Assign initial centroids
5. While metric(centroids, cases)>threshold
   - For i <= nb cases
     - Assign case to closest cluster according to metric
   - Recalculate centroids

### Input

| Variables | Description                              |
| --------- | ---------------------------------------- |
| DataSet   | File name of the collection of data.     |
| _k_       | Total number of dimensions               |
| _d_       | Distance function                        |
| threshold | Value for flexibility of the convergency |

### Main

Parse the arguments set them as configuration // For hadoop is put the in configuration object and in spark is to use sys.argv

centroids = Choose initial centroids RANDOMLY from the whole dataset
save centroids in memory/file

Initialize the counter of the converged centroids (converged_centroids = 0)

while converged_centroids < k :
converged_centroids = 0

Parse the arguments set them as configuration // For hadoop is put the in configuration object and in spark is to use sys.argv
centroids = Choose initial centroids
save centroids in memory/file

load_centroids():
centroids_list = []

for each centroids:
centroid = parse the centroid
add a centroid to the centroids_list

return centroids_list

while is not converged:

### Set Up

centroids = load the centroids from file/memory (load_centroids)

#### Map

method map(point):
point = parse the list of values from string (split to convert in and array of values)

    # decide to what centroid this point belongs to
    distance = INFINITY
    auxiliar_centroid = null

    for each centroid in centroids_list:
      euclidean_distance = find euclidean distance between centroid and point

      if euclidean_distance < distance:
        distance = euclidean_distance
        auxiliar_centroid = centroid

    EMIT(auxiliar_centroid, point)

#### Reduce

method reduce(centroid, points):
generate an empty vector (auxiliar_centroid) which the same dimension of the points

    1, [[ 1, 1, 1 ], [ 1, 1, 1 ], [ 1, 1, 1 ], [ 1, 1, 1 ], [ 1, 1, 1 ]]

    for each point
      sum all the coordinates and update the auxiliar_centroid

    auxiliar_centroid = [5, 5, 5]

    for each dimension
      calculate the mean value dividing the auxiliar_centroid value for dimension i by the number of points

    auxiliar_centroid = [
      5 / 5,
      5 / 5,
      5 / 5
    ]

    centroid = [1, 1, 1]
    auxiliar_centroid = [1, 1, 1]

    EMIT(index(centroid), values(auxiliar_centroid)) // save the centroids we found in file/memory

    find euclidean_distance between centroid and the auxiliar_centroid

    if euclidean_distance 0 <= threshold 0.5:
      converged_centroids = converged_centroids + 1

converged_centroids = 2
// LOOP FINISH

load_centroids():
centroids_list = []

for each centroids:
centroid = parse the centroid // "3,4,6" -> [3, 4, 6]
add a centroid to the centroids_list

return centroids_list
