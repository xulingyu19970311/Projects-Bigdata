import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object SparkHW{
    

   def well_form(sc: SparkContext)={
       val input = sc.textFile("/ds410/flightdata/2010-summary.csv")//input the file
       val da = input.filter(row => row !="DEST_COUNTRY_NAME,ORIGIN_COUNTRY_NAME,count")//remove header
       val first = da.filter(line => line.split(",").length == 3)//remove the commas
       first   
        }
   def well_formed(sc: SparkContext): Long ={
       val in = well_form(sc)//call
       in.count()//count the well-form line
       }
   def out_flight(sc: SparkContext)={
       val in = well_form(sc)
       val input =in.map(row => row.split(",")match{case Array(a,b,c) =>(a,c.toInt)})//trans to dest, count
       val f = input.aggregateByKey(0)(_+_,_+_)// group(a,1).... to (a,n)
       f
       }
   def in_flight(sc: SparkContext): Long={
        val in = well_form(sc)
        val input = in.map(row => row.split(",")match{case Array(a,b,c) =>(b,c.toInt)})
        val x = input.aggregateByKey(0)(_+_,_+_)
        val y = x.filter(j => j._2 >= 100)// filter if the in is >100
        y.count()
        }
    def trangles(sc: SparkContext): Long={
        val in = well_form(sc)
        val input =in.map(x => x.split(",")).map(x=>(x(0),x(1)))
        input.persist()
        val joined = input.join(input)
        val rev_joined = joined.map(x =>(x._2,x._1))
        val both_input = input.map(x => ((x._1,x._2),None))
        val combin = rev_joined.join(both_input)
        val re = combin.filter(x => x._1._2 != x._2._1).filter(x => x._1._1 != x._2._1).filter(x => x._1._1 != x._1._2)
        re.count()    
         }
    def net(sc: SparkContext)={
        val in = well_form(sc)
        val input = in.map(row => row.split(",")match{case Array(a,b,c) =>(a,c.toInt)})
        val x = input.aggregateByKey(0)(_+_,_+_)
        val input2 = in.map(row => row.split(",")match{case Array(a,b,c) =>(b,c.toInt)})
        val y = input2.aggregateByKey(0)(_+_,_+_)
        val z = x.join(y)// join 2 tables
        val j = z.map(x => (x._1 , x._2._2 - x._2._1))
        j
    } 
}




