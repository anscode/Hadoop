//order_id,order_date,order_customer_id,order_status
//order_item_id,order_item_order_id,order_item_product_id,order_item_quantity,order_item_subtotal,order_item_product_price

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

//using Dataset 
//Datasets: Type-safe operations.
//DataFrame: Untyped operations.
//Dataset vs. DataFrame
//A Dataset[T] is a parameterized type, where the type T is specified by the user and is associated with each element of the Dataset. 
//A DataFrame, on the other hand, has no explicit type associated with it at compile time, from the user's point of view. Internally, 
//a DataFrame is defined as a Dataset[Row], where Row is a generic row type defined by Spark SQL.
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
case class Order(OrderID : Int, OrderDate : String , CustomerID : Int, Status : String)
val order = orderFile.filter(f=> f != orderFirstRow).map(m=> m.split(",")).map(m=> Order(m(0).toInt, m(1), m(2).toInt,m(3))).toDS()
order.registerTempTable("Order")

case class OrderDetail(OrderItemID: Int, OrderID : Int,ProductID:Int, Quantity:Int,SubTotal:Float, Price: Float)
val orderDetail = orderDetailFile.filter(f => f != orderDetailFirstRow).map(m=>m.split(",")).map(m=>OrderDetail(m(0).toInt,m(1).toInt,m(2).toInt,m(3).toInt,m(4).toFloat,m(5).toFloat)).toDS()
orderDetail.registerTempTable("OrderDetail")

val orderByDate=sqlContext.sql("select o.OrderDate, sum(od.SubTotal) as Total from Order o inner join OrderDetail od on o.OrderID=od.OrderID group by o.OrderDate")



//The largest change that users will notice when upgrading to Spark SQL 1.3 is that SchemaRDD has been renamed to DataFrame.
//NOTE: lookup the difference between toDS() vs to DF()

