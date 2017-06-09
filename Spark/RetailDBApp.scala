import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

    //order_id,order_date,order_customer_id,order_status
    case class Order(orderID: Int, orderDate : String, customerID: Int, status: String)
    
    //order_item_id,order_item_order_id,order_item_product_id,order_item_quantity,order_item_subtotal,order_item_product_price
    case class OrderDetail(orderItemId:Int, orderID:Int,productID:Int,qty:Int,subTotal:Float,price:Float)

object RetailDBApp{
    
    val orderFileURL="C:\\bkp\\Data\\Git\\anscode\\Hadoop\\Data\\Retail_DB\\order.txt"
    val orderDetailFileURL ="C:\\bkp\\Data\\Git\\anscode\\Hadoop\\Data\\Retail_DB\\order_detail.txt"
    val saveLocaitonURL="C:\\bkp\\Data\\Git\\anscode\\Hadoop\\Data\\Retail_DB\\New"
   

    def main(args:Array[String])
    {
        println("==>Start of program<==")

        val conf= new SparkConf().setMaster("local").setAppName("RetailDBApp").set("spark.driver.allowMultipleContexts", "true")
        val sc= SparkContext.getOrCreate(conf)

        var loopOpt=true
        do{
            println("1. RDD")
            println("2. DF")
            val input= readInt()
            //val input=2
            println(s"Selected option $input")

            if(input==1)
                rddOperation(sc)
            else
                if(input==2)
                    dataSetOperation(sc)

            if(input==1 || input==2)
                loopOpt=false
            else
                println("Invalid selection")
        }while(loopOpt)

        println("==>End of program<==") 
    }

    def rddOperation(sc:SparkContext)
    {
        println("RDD operation")
        val orderFile=sc.textFile(orderFileURL)
        val orderFirst=orderFile.first()
        val order=orderFile.filter(o=>o != orderFirst).map(m=>(m.split(",")(0).toInt,m))
       
        val orderDetailFile=sc.textFile(orderDetailFileURL)
        val orderDetailFirst=orderDetailFile.first()
        val orderDetail=orderDetailFile.filter(f=>f!=orderDetailFirst).map(m=>(m.split(",")(1).toInt,m))

        val orderOrderDetailJoin=order.join(orderDetail)
        val orderDateAndPrice=orderOrderDetailJoin.map(m=>(m._2._1.split(",")(1),m._2._2.split(",")(4).toFloat))
        val orderTotalByDate=orderDateAndPrice.reduceByKey((x,y)=>x+y)
        orderTotalByDate.filter(f=>f._1=="2008-01-31 00:00:00").collect().foreach(println)
        orderTotalByDate.map(t=>t._1+"\t"+t._2).saveAsTextFile(saveLocaitonURL) //save file tab delimited

        
    }

    def dataSetOperation(sc:SparkContext){
        println("DataSet operation")
        val sqlContext= new SQLContext(sc)

        val orderFile=sc.textFile(orderFileURL)
        val orderFirst=orderFile.first()
        val order=orderFile.filter(f=>f!=orderFirst).map(m=>m.split(",")).map(m=>Order(m(0).toInt,m(1),m(2).toInt,m(3))).toDS()
        order.registerTempTable("Order")

        val orderDetailFile=sc.textFile(orderDetailFileURL)
        val orderDetailFirst=orderDetailFile.first()
        val orderDetail=orderDetailFile.filter(f=>f!=orderDetailFirst).map(m=>m.split(",")).map(m=>OrderDetail(m(0).toInt,m(1).toInt,m(2).toInt,m(3).toInt,m(4).toFloat,m(5).toFloat)).toDS()
        orderDetail.registerTempTable("OrderDetail")

        val query=sqlContext.sql("select O.orderDate,sum(D.price) from Order O inner join OrderDetail D on O.OrderID=D.OrderID where o.orderDate='2008-01-31 00:00:00' group by O.orderDate")
        query.collect().foreach(println)
    }

}