import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object RetailDBApp{

    def main(args:Array[String])
    {
        println("==>Start of App<==")
        val conf=new SparkConf().setAppName("RetailDB App").setMaster("local")
        val sc= SparkContext.getOrCreate(conf)




        print("==>End of App<==")
    }
}