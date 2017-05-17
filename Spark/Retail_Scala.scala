val orderFileUrl="C:\\bkp\\Data\\Retail_Db\\order.txt"

val orderDetailFileUrl = "C:\\bkp\\Data\\Retail_Db\\order_detail.txt"

val orderFile = sc.textFile(orderFileUrl)
val orderFirstRow = orderFile.first()

val orderDetailFile = sc.textFile(orderDetailFileUrl)
val orderDetailFirstRow =orderDetailFile.first()


//RDD
val orderRDD = orderFile.filter(f => f != orderFirstRow).map(m=>(m.split(",")(0).toInt, m))

val orderDetailRDD =orderDetailFile.filter(f => f!= orderDetailFirstRow).map(m=> (m.split(",")(0).toInt,m))

val orderJoin= orderRDD.join(orderDetailRDD)

val orderDTAmountRDD =orderJoin.map(m=> (m._2._1.split(",")(1), m._2._2.split(",")(4).toFloat) )

val orderByDateRDD = orderDTAmountRDD.reduceByKey((x,y) => x+y)

orderByDateRDD.collect().foreach(println)
