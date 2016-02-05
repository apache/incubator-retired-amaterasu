val data = Array(1, 2, 3, 4, 5)
val x= data.tail

val rdd = sc.parallelize(data)