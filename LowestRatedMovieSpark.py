from pyspark import SparkConf, SparkContext


def load_movies_data():
    data = {}
    with open("u.item", "r") as file:
        for line in file:
            line = line.split('|')
            data[int(line[0])] = line[1]
    return data


def load_ratings_data():
    data = {}
    with open("u.data", "r") as file:
        for line in file:
            line = line.split('\t')
            try:
                data[int(line[1])].append((line[0], line[2]))
            except KeyError:
                data[int(line[1])] = [(line[0], line[2])]
    return data


def process_line(line):
    line = line.split()
    return int(line[1]), (line[2], 1.0)


if __name__ == "__main__":
    conf = SparkConf().setAppName("WorstMovies")
    sc = SparkContext(conf=conf)

    moviesNameData = load_movies_data()
    ratingsData = sc.textFile("hdfs:///data/u.data")
    # Convert to (movieId,(rating,1.0 : for Count))
    ratingsData = ratingsData.map(process_line)

    # reduce to (moviesId , (sumRating,sumCount))
    reducedRatingData = ratingsData.reduceByKey(
        lambda rating1, rating2: (rating1[0] + rating2[1], rating1[0] + rating2[1]))
    print(reducedRatingData.take(5))
    results = reducedRatingData.mapValues(
        lambda sum_rating_and_count: sum_rating_and_count[0] / sum_rating_and_count[1])
    print(results.take(5))
    sortedResults = results.sortBy(lambda x: x[1])

    finalResults = sortedResults.take(5)

    for rating in finalResults:
        print(moviesNameData[rating[0]], rating[1])
