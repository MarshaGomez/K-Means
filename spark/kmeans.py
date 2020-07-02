import sys
import argparse

from operator import add
from math import pow, sqrt
from pyspark import SparkContext


# Setting up arguments parser
parser = argparse.ArgumentParser(description='Process gnocchi requests.')

parser.add_argument('--k', type=int, default=2, help='Total number of dimensions.')
parser.add_argument('--d', type=int, default=2, help='Distance function.')
parser.add_argument('--t', default=0.1, help='Threshold. Value for flexibility of the convergency.')
parser.add_argument('--max_iteration', default=15, help='Date gnocchi use for start the process.')
parser.add_argument('--i', default="ec021b1a-9156-4011-9b62-8d861d4b6eb3", help='Metric ID. Use by default Huminity Metric ID.')
parser.add_argument('--o', default="ec021b1a-9156-4011-9b62-8d861d4b6eb3", help='Metric ID. Use by default Huminity Metric ID.')

args = parser.parse_args()



def print_arguments(argv):
    print("======PARAMETERS======")
    print("K:\t\t" + argv[2])
    print("DIMENSION:\t" + argv[3])
    print("THRESHOLD:\t" + argv[4])
    print("OUTPUT FILE:\t" + argv[6])
    print("======================")

def parsePoints(text, dimension):
    pointsText = text.split(',')[:dimension]
    points = [float(i) for i in pointsText]

    return points

def get_euclidean_distance(pointA, pointB):
    sum = 0.0

    for index in range(len(pointA)):
        difference = pointA[index] - pointB[index]
        sum += pow(difference, 2)

    return sqrt(sum)

def assign_centroid(centroids, point):
    closest_centroid_index = -1
    min_distance = float('inf')

    for i in range(len(centroids)):
        distance = get_euclidean_distance(centroids[i], point)

        if min_distance > distance:
            closest_centroid_index = i
            min_distance = distance

    return (closest_centroid_index, point)

def get_mean_centroid(element):
    index = 0
    mean_centroid = None

    for centroid in element[1]:
        if index == 0:
            mean_centroid = centroid
        else:
            for index in len(centroid):
                mean_centroid[index] += centroid[index]

    return mean_centroid

# def g(x):
#     for element in x[1]:
#         print(x[0], element)

    # print()

if __name__ == "__main__":
    if len(sys.argv) < 7:
        print("Usage: kmeans <input file> <k> <dimension> <threshold> <centersFilename> [<output file>]", file=sys.stderr)
        sys.exit(-1)

    master = "local"
    sc = SparkContext(master, "Kmeans")

    print_arguments(sys.argv)

    lines = sc.textFile(sys.argv[1])
    k = int(sys.argv[2])
    dimension = int(sys.argv[3])
    threshold = float(sys.argv[4])
    output_file = sys.argv[6]

    points =  lines.map(lambda x: parsePoints(x, dimension))
    random_centroids = points.takeSample(False, k)
    initial_centroids = [element[:dimension] for element in random_centroids]
    clusters_points = points.map(lambda point: assign_centroid(initial_centroids, point))
    clusters = clusters_points.groupByKey().collect()
    parallelize_clusters = sc.parallelize(clusters)
    new_centroids = parallelize_clusters.map(lambda cluster: get_mean_centroid(cluster)).collect()

    converge_count = 0

    print("INITIAL CENTROIDS")
    print(initial_centroids)
    print("NEW CENTROIDS")
    print(new_centroids)

    for index in range(len(initial_centroids)):
        if get_euclidean_distance(initial_centroids[index], new_centroids[index]) < threshold:
            converge_count+=1

    print("CONVERGED CENTROIDS: " + str(converge_count))


    # coordinates = lines.flatMap(lambda x: x.split(','))
    # print("::HOLA::")

    # print(random_centroids)
    # print(points)
    # points.foreach(g)

    # counts = ones.reduceByKey(add)

    # if len(sys.argv) == 3:
    #     counts.repartition(1).saveAsTextFile(sys.argv[2])
    # else:
    #     output = counts.collect()
    #     for (word, count) in output:
    #         print("%s: %i" % (word, count))
