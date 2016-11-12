data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
rdd = AmaContext.sc.parallelize(data)
odd = rdd.filter(lambda num: num % 2 != 0)