import blast.data_processing.evaluation
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object reproduce_data {
  // this is to directly read the blocking graph and perform evaluation
  // the blast.main must have been executed so there would be a blocking graph object
  def main(args: Array[String]): Unit ={
    val spark = SparkSession.builder
      .appName(s"Blast")
      .master("local[*]")
      .getOrCreate()

    //for PRD
    val blgraphPath = "/media/sf_uniassignments/BLAST/blocking_graphs/abt_buy"
    val groundtruth = "/media/sf_uniassignments/BLAST/groundtruth_abt_buy"
    //for ar1

    println("Reading from blocking graph:")
    val blgraph : RDD[(String,String)]= spark.sparkContext.objectFile(blgraphPath)
    val (recall, precision) = evaluation.evaluate(blgraph,groundtruth)
    println("Recall: " + (BigDecimal(recall).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble) * 100 + "%")
    println("Precision: " + (BigDecimal(precision).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble) * 100+ "%")
  }
}
