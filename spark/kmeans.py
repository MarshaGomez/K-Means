
import sys
import time
from math import pow, sqrt
from pyspark import SparkContext

def parsePoint(line, dimension):
  line = line.split(',')[0:dimension]
  result = [float(i) for i in line]

  return result

def calculate_euclidean_distance(pointA, pointB):
  sum = 0.0

  for index in range(len(pointA)):
    difference = pow(pointA[index] - pointB[index], 2)
    sum += difference

  return sqrt(sum)
 
def assign_nearest_centroid(point, centroids):
  centroid_index = -1
  minimum_distance = float('inf')

  for index in range(len(centroids)):
    distance = calculate_euclidean_distance(centroids[index], point)
    
    if (distance < minimum_distance):
      centroid_index = index
      minimum_distance = distance

  return (centroid_index, point)

def get_mean_centroids(cluster):
  mean_centroid = []
  aux_index = 0
  
  for point in cluster[1]:
    if aux_index == 0:
      mean_centroid = point
    else:
      for array_index in range(len(point)):
        mean_centroid[array_index] += point[array_index]
       
    aux_index+=1
  
  size = len(list(cluster[1]))
  mean_centroid = [(coordinate / size) for coordinate in mean_centroid]

  return (cluster[0], mean_centroid)


if __name__ == "__main__":
  if len(sys.argv) < 5:
    print("=========================================================================")
    print("Usage: kmeans <input file> <k> <dimension> <threshold> [<centersFilename>]", file=sys.stderr)
    print("=========================================================================")
    sys.exit(-1)

  print("=======================")
  print("input file: " + sys.argv[1])
  print("=======================")
  print("K: " + sys.argv[2])
  print("=======================")
  print("dimension: " + sys.argv[3])
  print("=======================")
  print("threshold: " + sys.argv[4])
  print("=======================")

  master = "yarn"
  sc = SparkContext(master, "Kmeans SPARK")
  
  k = int(sys.argv[2])
  dimension = int(sys.argv[3])
  threshold = float(sys.argv[4])
  lines = sc.textFile(sys.argv[1])
  convergence_count = 0
  iteration_count = 0
  #remember cache
  points = lines.map(lambda x: parsePoint(x, dimension))# .cache()
  random_centroids = []

  while (convergence_count < k):
    convergence_count = 0

    if iteration_count == 0:
      #comment line 88
      random_centroids = points.takeSample(False, k)
      # Be careful with dimension and K
      #d = 3 k = 7
      #random_centroids = [[0.297959,0.474801,0.448468], [0.297959,0.480106,0.214485], [0.297959,0.482759,0.247911], [0.297959,0.482759,0.51532], [0.297959,0.469496,0.211699], [0.297959,0.458886,0.220056], [0.297959,0.453581,0.239554]]
      # d = 3 k = 13
      #random_centroids = [[0.297959,0.474801,0.448468], [0.297959,0.480106,0.214485], [0.297959,0.482759,0.247911], [0.297959,0.482759,0.51532], [0.297959,0.469496,0.211699], [0.297959,0.458886,0.220056], [0.297959,0.453581,0.239554],[0.297959,0.450928,0.292479], [0.297959,0.450928,0.259053], [0.297959,0.450928,0.401114], [0.293878,0.464191,0.292479], [0.293878,0.477454,0.32312], [0.293878,0.482759,0.395543]]
      # d = 7 k = 7
      #random_centroids = [[0.297959,0.474801,0.448468,0.024768,0.598886,0.038997,0.119777], [0.297959,0.480106,0.214485,0.021672,0.398329,0.030641,0.902507], [0.297959,0.482759,0.247911, 0.037152,0.311978,0.041783,0.033426], [0.297959,0.482759,0.51532,0.012384,0.724234,0.02507,0.278552], [0.297959,0.469496,0.211699,0.077399,0.256267,0.08078,0.169916], [0.297959,0.458886,0.220056,0.074303,0.247911,0.072423,0.172702], [0.297959,0.453581,0.239554,0.06192,0.256267,0.064067,0.208914]]
      # d = 7 k = 13
      #random_centroids = [[0.297959,0.474801,0.448468,0.024768,0.598886,0.038997,0.119777], [0.297959,0.480106,0.214485,0.021672,0.398329,0.030641,0.902507], [0.297959,0.482759,0.247911,0.037152,0.311978,0.041783,0.033426], [0.297959,0.482759,0.51532,0.012384,0.724234,0.02507,0.278552], [0.297959,0.469496,0.211699,0.077399,0.256267,0.08078,0.169916], [0.297959,0.458886,0.220056,0.074303,0.247911,0.072423,0.172702], [0.297959,0.453581,0.239554,0.06192,0.256267,0.064067,0.208914],[0.297959,0.450928,0.292479,0.043344,0.348189,0.050139,0.228412], [0.297959,0.450928,0.259053,0.012384,0.350975,0.016713,0.038997], [0.297959,0.450928,0.401114,0.037152,0.465181,0.047354,0.320334], [0.293878,0.464191,0.292479,0.049536,0.350975,0.052925,0.256267], [0.293878,0.477454,0.32312,0.055728,0.398329,0.072423,0.289694], [0.293878,0.482759,0.395543,0.034056,0.557103,0.050139,0.259053]]

      print("=======================")
      print("RANDOM CENTROIDS::")
      print("=======================")
      print(random_centroids)
      print("=======================")
    else:
      random_centroids = [mean_centroid[1] for mean_centroid in mean_centroids]
    
    clusters_points = points.map(lambda x: assign_nearest_centroid(x, random_centroids))
    clusters = clusters_points.groupByKey()
    mean_centroids = clusters.map(lambda cluster: get_mean_centroids(cluster)).collect()

    for index in range(len(mean_centroids)):
      mean_centroid_index = mean_centroids[index][0]
      mean_centroid_points = mean_centroids[index][1]
      
      distance = calculate_euclidean_distance(mean_centroid_points, random_centroids[mean_centroid_index])
      
      if (distance <= threshold):
        convergence_count+=1
    
    iteration_count+=1
  
  print("MEAN CENTROIDS::")
  print("=======================")
  print(mean_centroids)

  
  print("=======================")
  print("CONVERGED CENTROIDS COUNT: " + str(convergence_count))
  print("=======================")
  print("ITERATIONS COUNT: " + str(iteration_count))
  print("=======================")

  

  # print(clusters.collect())

  # print("RANDOM CENTROIDS PICKED")
  # print(random_centroids)
  # print("CLUSTERS_CENTROIDS")
  # print(clusters_points.collect())

# spark-submit --master yarn my_script.py

# spark-submit kmeans.py data.txt 3 3 0.5 centroids.txt output

#THRESHOLD 0.05

# test point n = 1.000
# spark-submit kmeans.py points-1k.txt 7 3 0.05 centroids.txt output (ok)
# spark-submit kmeans.py points-1k.txt 13 3 0.05 centroids.txt output (ok)
# spark-submit kmeans.py points-1k.txt 7 7 0.05 centroids.txt output (ok)
# spark-submit kmeans.py points-1k.txt 13 7 0.05 centroids.txt output (ok)

# test point n = 10.000
# spark-submit kmeans.py points-10k.txt 7 3 0.05 centroids.txt output (ok)
# spark-submit kmeans.py points-10k.txt 13 3  centroids.txt output (ok)
# spark-submit kmeans.py points-10k.txt 7 7 0.05 centroids.txt output (ok)
# spark-submit kmeans.py points-10k.txt 13 7 0.05 centroids.txt output (ok)


# test point n = 100.000
# spark-submit kmeans.py points-100k.txt 7 3 0.05 centroids.txt output(ok)
# spark-submit kmeans.py points-100k.txt 13 3 0.05 centroids.txt output (ok)
# spark-submit kmeans.py points-100k.txt 7 7 0.05 centroids.txt output (ok)
# spark-submit kmeans.py points-100k.txt 13 7 0.05 centroids.txt output (ok)

#THRESHOLD 0.0001

# test point n = 1.000
# spark-submit kmeans.py points-1k.txt 7 3 0.0001 centroids.txt output (ok) 
# spark-submit kmeans.py points-1k.txt 13 3 0.0001 centroids.txt output (ok) 
# spark-submit kmeans.py points-1k.txt 7 7 0.0001 centroids.txt output (ok) 
# spark-submit kmeans.py points-1k.txt 13 7 0.0001 centroids.txt output (ok) 

# test point n = 10.000
# spark-submit kmeans.py points-10k.txt 7 3 0.0001 centroids.txt output (ok) 
# spark-submit kmeans.py points-10k.txt 13 3 0.0001 centroids.txt output (ok) 
# spark-submit kmeans.py points-10k.txt 7 7 0.0001 centroids.txt output (ok) 
# spark-submit kmeans.py points-10k.txt 13 7 0.0001 centroids.txt output (ok) 


# test point n = 100.000
# spark-submit kmeans.py points-100k.txt 7 3 0.0001 centroids.txt output(ok) 
# spark-submit kmeans.py points-100k.txt 13 3 0.0001 centroids.txt output (ok) 
# spark-submit kmeans.py points-100k.txt 7 7 0.0001 centroids.txt output (ok) 
# spark-submit kmeans.py points-100k.txt 13 7 0.0001 centroids.txt output (ok)

# random_Centroids::
# [[0.297959, 0.469496, 0.211699], [0.297959, 0.474801, 0.448468], [0.297959, 0.450928, 0.401114]]
# 
# mean_centroids::
# 
# [(1, [0.297959, 0.47878, 0.481894]), (0, [0.29795900000000003, 0.463812, 0.24074814285714283]), (2, [0.297959, 0.450928, 0.401114])]

#[(0, [0.297959, 0.46949599999999997, 0.45496733333333333]), (1, [0.29795900000000003, 0.46595933333333334, 0.23212633333333332]), (2, [0.297959, 0.450928, 0.292479])]


#[[0.297959, 0.450928, 0.401114], [0.297959, 0.450928, 0.292479], [0.297959, 0.453581, 0.239554]]


#RANDOM CENTROIDS PICKED
#[[0.297959, 0.450928, 0.259053], [0.297959, 0.450928, 0.292479], [0.297959, 0.450928, 0.401114]]
#CLUSTERS_CENTROIDS

#(0, [0.297959, 0.453581, 0.239554]), (1, [0.297959, 0.450928, 0.292479]), (0, [0.297959, 0.450928, 0.259053]), (2, [0.297959, 0.450928, 0.401114])]

#  [0.297959, 0.474801, 0.448468]

#  [0.297959 + 0.297959, 0.474801 +  0.482759, 0.448468 + 0.51532]


# COLLECTED CLUSTERS: 0
# [0.297959, 0.474801, 0.448468]
# [0.297959, 0.482759, 0.51532]
# [0.297959, 0.450928, 0.401114]
# COLLECTED CLUSTERS: 1
# [0.297959, 0.480106, 0.214485]
# [0.297959, 0.469496, 0.211699]
# [0.297959, 0.458886, 0.220056]
# COLLECTED CLUSTERS: 2
# [0.297959, 0.482759, 0.247911]
# [0.297959, 0.453581, 0.239554]
# [0.297959, 0.450928, 0.292479]
# [0.297959, 0.450928, 0.259053]
