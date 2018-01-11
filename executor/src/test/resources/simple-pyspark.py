data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
try:
    rdd = sc.parallelize(data)

    def g(x):
        print(x)

    rdd.foreach(g)
except Exception as e:
    print type(e), e