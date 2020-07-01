# Workgroup Activity

## Table of contents

1. [The basic idea](#1-the-basic-idea)
2. [The Algorithm](#2-the-algorithm)
3. [Implementation](#3-implementation)

## 1. The basic Idea

K-Means is one of the most popular "clustering" algorithms. K-means stores $k$ centroids that it uses to define clusters. A point is considered to be in a particular cluster if it is closer to that cluster's centroid than any other centroid.

K-Means finds the best centroids by alternating between (1) assigning data points to clusters based on the current centroids (2) chosing centroids (points which are the center of a cluster) based on the current assignment of data points to clusters.

## 2. The Algorithm

In the clustering problem, we are given a training set ${x^{(1)}, ... , x^{(m)}}$, and want to group the data into a few cohesive "clusters." Here, we are given feature vectors for each data point $x^{(i)} \in \mathbb{R}^n$ as usual; but no labels $y^{(i)}$ (making this an unsupervised learning problem). Our goal is to predict $k$ centroids and a label $c^{(i)}$ for each datapoint. The k-means clustering algorithm is as follows:

## 3. Implementation

Here is pseudo-python code which runs k-means on a dataset. It is a short algorithm made longer by verbose commenting.

INPUT:

- dataSet : filename where the data is.
- k : number of clusters
- condition : (TODO)

Main

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

# SET UP

centroids = load the centroids from file/memory (load_centroids)

# MAP

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

# REDUCE

save the centroids we found in file/memory
