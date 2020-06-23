# The k-means Clustering Algorithm in MapReduce

The problem of partitioning a set of unlabeled points into clusters appears in a wide variety of applications. One of the most well-known and widely used clustering algorithms is Lloyd's algorithm, commonly referred to simply as the *k-means* clustering algorithm. The popularity of *k-means* is due in part to its simplicity - the only parameter which needs to be chosen is *k*, the desired number of clusters - and also its speed

Let ![equation](http://www.sciweavers.org/upload/Tex2Img_1592905961/render.png) be a set of *n* data points, each with dimension *d*. The *k-means* problem seeks to find a set of *k* points (called means) ![equation](http://www.sciweavers.org/upload/Tex2Img_1592906204/render.png) which minimizes the function


![equation](http://www.sciweavers.org/upload/Tex2Img_1592906739/render.png)

In other words, we wish to choose *k* means so as to minimize the sum of the squared distances between each point in the data set and the mean closest to that point.
Finding an exact solution to this problem is *NP-hard*. However, there are a number of heuristic algorithms which yield good approximate solutions. The standard algorithm for solving the *k-means* problem uses an iterative process which guarantees a decrease in total error (value of the objective function *f(M)*) on each step. The algorithm is as follows:





