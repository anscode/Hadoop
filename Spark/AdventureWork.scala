// Spark
import org.apache.spark.SparkContext
import SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext


//for date time
import java.text.SimpleDateFormat
import java.util.Calendar


case class Customer(CustomerKey:Int,FirstName:String,MiddleName:String,LastName:String,Gender:String,Email:String);

case class Sales(CustomerKey:Int,ProductKey:Int,Total:String);

case class Product(ProductKey:Int,Category:String,ProductName:String);

object AdventureWork{
		 val customerFile="C:\\bkp\\data\\AdventureWorks\\customer.txt"
		 val salesFile="C:\\bkp\\data\\AdventureWorks\\sales.txt"
		 var productFile="C:\\bkp\\data\\AdventureWorks\\product.txt"
		 var outFile="C:\\bkp\\data\\AdventureWorks\\resultout_" + getFileName
		 

	def main(args:Array[String]){
		 
		/*
		 val conf = new SparkConf().setAppName("AdventureWork").setMaster("local").set("spark.driver.allowMultipleContexts", "true")
		 val sc = new SparkContext(conf)
		 */
		 val sc= SparkContext.getOrCreate()
		 
		 //val opt=Console.readInt()
		 //println(opt) 
		 
	
		  //executeRDD_Class(sc,customerFile, salesFile,productFile,outFile)
		  executeRDD(sc,customerFile, salesFile,productFile,outFile)
		  //executeDF()
		
	
	}
	
	def executeRDD(sc: SparkContext, customerFile: String, salesFile:String, productFile:String, outFile:String){
		  val customer=sc.textFile(customerFile)
		  val customerFirstRow=customer.first()
		  //customer.filter(f =>f != customerFirstRow).map(c=>c.split('\t')).map(r=>(r(0).toInt,r)).collect().foreach(p=>(println(">>> key=" + p._1 + ", value=" + p._2)))
		  val custData = customer.filter(f =>f != customerFirstRow).map(c=>c.split('\t')).map(r=>(r(0).toInt,r))
		  
		  val sales=sc.textFile(salesFile)
		  val salesFirstRow=sales.first()
		  val salesData = sales.filter(f=>f != salesFirstRow).map(m=>m.split('\t')).map(r=>(r(0).toInt,r))
		  
		  val customerSalesJoin= custData.join(salesData)
		  
		  customerSalesJoin.saveAsTextFile(outFile)
		  customerSalesJoin.collect().foreach(println)
		  
		  
	}
	
	def executeDF()
	{
		val spark = SparkSession.builder().master("local").appName("Word Count").getOrCreate()
		val dfCustomer =spark.read
						.option("header", "true")
						.option("delimiter","\t")
						.option("inferSchema", "true") 
						.csv("c:\\bkp\\data\\adventureworks\\customer.txt")
		//dfCustomer.printSchema
		dfCustomer.createOrReplaceTempView("customer");
		
		val dfSale =spark.read
						.option("header", "true")
						.option("delimiter","\t")
						.option("inferSchema", "true") 
						.csv("c:\\bkp\\data\\adventureworks\\sales.txt")
		//dfSale.printSchema
		dfSale.createOrReplaceTempView("sale");
		
		/*
		spark.sql("select * from Customer").show()
		spark.sql("select * from Sale").show()
		
		spark.sql("select Customer.CustomerID, Sale.ProductID,Sale.Total from Customer inner join Sale on Customer.CustomerID=Sale.CustomerID where Customer.CustomerID=11000").show()
		
		spark.sql("select CustomerID, sum(Total) as Total from Sale group by CustomerID").show()
			*/
		spark.sql("select c.CustomerID, c.FName,c.LName, s.Total from Customer c inner join (select CustomerID, sum(Total) as Total from Sale group by CustomerID) as s on c.CustomerID=s.CustomerID order by s.Total").show()

		var cust=spark.sql("select * from Customer")

		//cust.write.parquet("employee.parquet")

		
	}
	
	def executeRDD_Class(sc: SparkContext, customerFile: String, salesFile:String, productFile:String, output:String){
		
		val custInput=sc.textFile(customerFile)
		val custFirstRow=custInput.first()
		//val customerData=custFirstRow.collect()
		var customerData=custInput.filter(f=>f != custFirstRow).map(r=>r.split("\t")).map(r=> (r(0),Customer(r(0).toInt,r(1),r(2),r(3),r(4),r(5))));
		
		val salesInput=sc.textFile(salesFile)
		val salesFirstRow=salesInput.first()
		val salesData=salesInput.filter(f=>f != salesFirstRow).map(r=>r.split("\t")).map(r=>(r(0),Sales(r(0).toInt,r(1).toInt,r(2))));
 
		val customerSales= customerData.join(salesData);
		
		customerSales.saveAsTextFile(output)
		customerSales.collect().foreach(println)
	}
	
	  private def getFileName :String={
	 //new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SS").format(Calendar.getInstance().getTime())
	new SimpleDateFormat("ddMMyyyy_HHmmss_SS").format(Calendar.getInstance().getTime())
  }
}