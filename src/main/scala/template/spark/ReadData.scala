package template.spark

import org.apache.spark.sql.DataFrame
import template.spark.Main.spark

trait GetDataframe {
  def getData(fileName:String): DataFrame ={
    fileName.toLowerCase match {
      case "drivers" => spark.read.format("csv").option("header", "true").option("inferSchema", "true").csv("data/input/drivers.csv")
      case "results" => spark.read.format("csv").option("header", "true").option("inferSchema", "true").csv("data/input/results.csv")
    }
  }

}
