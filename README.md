# The k-means Clustering Algorithm in MapReduce

The problem of partitioning a set of unlabeled points into clusters appears in a wide variety of applications. One of the most well-known and widely used clustering algorithms is Lloyd's algorithm, commonly referred to simply as the *k-means* clustering algorithm. The popularity of *k-means* is due in part to its simplicity - the only parameter which needs to be chosen is *k*, the desired number of clusters - and also its speed

Let ![equation](http://www.sciweavers.org/upload/Tex2Img_1592905961/render.png) be a set of *n* data points, each with dimension *d*. The *k-means* problem seeks to find a set of *k* points (called means) ![equation](http://www.sciweavers.org/upload/Tex2Img_1592906204/render.png) which minimizes the function


![equation](http://www.sciweavers.org/upload/Tex2Img_1592906739/render.png)

In other words, we wish to choose *k* means so as to minimize the sum of the squared distances between each point in the data set and the mean closest to that point.
Finding an exact solution to this problem is *NP-hard*. However, there are a number of heuristic algorithms which yield good approximate solutions. The standard algorithm for solving the *k-means* problem uses an iterative process which guarantees a decrease in total error (value of the objective function *f(M)*) on each step. The algorithm is as follows:

![Image of Yaktocat](k-means.PNG)

In other words, the *k-means* algoritm chooses *k* initial means ![equation](http://www.sciweavers.org/upload/Tex2Img_1592952296/render.png) uniformly at random from the set *X* (line 1). Then, for each point ![equation](http://www.sciweavers.org/upload/Tex2Img_1592952368/render.png), it finds the closest mean ![equation](http://www.sciweavers.org/upload/Tex2Img_1592952501/render.png) and adds x to a set ![equation](http://www.sciweavers.org/upload/Tex2Img_1592952587/render.png) (lines 5-7) initially empty (lines 3-4). Then, for each mean ![equation](http://www.sciweavers.org/upload/Tex2Img_1592952672/render.png), it recomputes the mean value to be the the centroid of the data points in ![equation](http://www.sciweavers.org/upload/Tex2Img_1592952977/render.png) (lines 8-9).
These steps are repeated until the means have converged (line 2). The convergence criterion is typically when the total error stops changing between steps, in which case a local optimum of the objective function has been reached.
However, some implementations terminate the search when the change in error between iterations drops below a certain threshold. Each iteration of this algorithm takes time *O(nkd)*. In principle, the number of iterations required for the algorithm to fully converge can be very large, but on real datasets the algorithm typically converges in at most a few dozen iterations.
In this project you must:
1. Design a MapReduce algorithm (using pseudocode) to implement the *k-means* algorithm;
2. Implement the designed MapReduce algorithm using the Hadoop framework;
3. Implement the designed MapReduce algorithm using the Spark framework;
4. Test both implementations on a synthetic or real-world dataset;
5. Write a project report detailing your design, implementations and reporting the experimental results.
For higher marks, please address efficiency issues in your implementation; examples include, but are not limited to, the following:
* Use combiners and/or more than 1 reducer;
* Use custom WritableComparable objects;
* Use the Mapper and Reducer classes adequately;
* Test your implementations on a range of different datasets, i.e., *n* = 1,000, 10,000 and 100,000, *d* = 3 and 7, and *k* = 7 and 13.
