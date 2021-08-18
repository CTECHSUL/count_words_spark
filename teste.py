from pyspark import SparkContext, SparkConf


sc = SparkContext('local')

words = sc.textFile("livro.txt").flatMap(lambda line: line.split(" "))

wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b).sortBy(lambda a:a[1], ascending=False)
wordCounts.saveAsTextFile("resultado")