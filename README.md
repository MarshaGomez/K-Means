# The k-means Clustering Algorithm in MapReduce

The problem of partitioning a set of unlabeled points into clusters appears in a wide variety of applications. One of the most well-known and widely used clustering algorithms is Lloyd's algorithm, commonly referred to simply as the _k-means_ clustering algorithm. The popularity of _k-means_ is due in part to its simplicity - the only parameter which needs to be chosen is _k_, the desired number of clusters - and also its speed

Let ![equation](https://render.githubusercontent.com/render/math?math=X%3D%5Cleft%5C%7Bx_%7B1%7D%2C...%2Cx_%7Bn%7D%5Cright%5C%7D) be a set of _n_ data points, each with dimension _d_. The _k-means_ problem seeks to find a set of _k_ points (called means) ![equation](https://render.githubusercontent.com/render/math?math=M%3D%5Cleft%5C%7B%5Cmu_%7B1%7D%2C...%2C%5Cmu_%7Bk%7D%5Cright%5C%7D) which minimizes the function

![equation](<https://render.githubusercontent.com/render/math?math=f(M)%20=%20\sum_{x\epsilon%20X}%20min_{\mu%20\epsilon%20M}%20\left%20|%20x%20-%20\mu%20%20\right%20|\frac{2}{2}>)

In other words, we wish to choose _k_ means so as to minimize the sum of the squared distances between each point in the data set and the mean closest to that point.
Finding an exact solution to this problem is _NP-hard_. However, there are a number of heuristic algorithms which yield good approximate solutions. The standard algorithm for solving the _k-means_ problem uses an iterative process which guarantees a decrease in total error (value of the objective function _f(M)_) on each step. The algorithm is as follows:

![Image of Yaktocat](k-means.PNG)

In other words, the _k-means_ algoritm chooses _k_ initial means ![equation](https://render.githubusercontent.com/render/math?math=%5Cmu_%7B1%7D%2C...%2C%5Cmu_%7Bk%7D) uniformly at random from the set _X_ (line 1). Then, for each point ![equation](https://render.githubusercontent.com/render/math?math=x%5Cepsilon%20X), it finds the closest mean ![equation](https://render.githubusercontent.com/render/math?math=%5Cmu_%7B1%7D) and adds _x_ to a set ![equation](https://render.githubusercontent.com/render/math?math=%5Comega_%7Bc%7D) (lines 5-7) initially empty (lines 3-4). Then, for each mean ![equation](https://render.githubusercontent.com/render/math?math=%5Cmu_%7Bi%7D), it recomputes the mean value to be the the centroid of the data points in ![equation](https://render.githubusercontent.com/render/math?math=%5Comega_%7Bi%7D) (lines 8-9).
These steps are repeated until the means have converged (line 2). The convergence criterion is typically when the total error stops changing between steps, in which case a local optimum of the objective function has been reached.
However, some implementations terminate the search when the change in error between iterations drops below a certain threshold. Each iteration of this algorithm takes time _O(nkd)_. In principle, the number of iterations required for the algorithm to fully converge can be very large, but on real datasets the algorithm typically converges in at most a few dozen iterations.

In this project you must:

1. Design a MapReduce algorithm (using pseudocode) to implement the _k-means_ algorithm;
2. Implement the designed MapReduce algorithm using the Hadoop framework;
3. Implement the designed MapReduce algorithm using the Spark framework;
4. Test both implementations on a synthetic or real-world dataset;
5. Write a project report detailing your design, implementations and reporting the experimental results.

For higher marks, please address efficiency issues in your implementation; examples include, but are not limited to, the following:

- Use combiners and/or more than 1 reducer;
- Use custom WritableComparable objects;
- Use the Mapper and Reducer classes adequately;
- Test your implementations on a range of different datasets, i.e., _n_ = 1,000, 10,000 and 100,000, _d_ = 3 and 7, and _k_ = 7 and 13.

## Group goals

1. How Choose initial centroids ()
2. Map logic (+)
3. Reduce logic ()
4. Create hadoop project ()
5. Create spark project ()

---

## ALGORITHMS FOR INITIAL CENTROID:

- Hartigan and Wong
- Pick from the all set k points, doing a loop and pick randonly k points
- pick the first k points
- Create random k points with the process you create the dataset (use the same function of generator)

HOMEWORK:

- Hartigan and Wong
- Preprocess the dataset (weather dataset)

Datasets

- Weather dataset: (https://www.kaggle.com/prakharrathi25/weather-data-clustering-using-k-means)
- Online retail dataset (https://www.kaggle.com/hellbuoy/online-retail-k-means-hierarchical-clustering)
- Customer Segmentation: (https://www.kaggle.com/biphili/customer-centricity-k-means)
