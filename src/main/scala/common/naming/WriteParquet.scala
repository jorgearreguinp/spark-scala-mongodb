package common.naming

import common.Constants.{APP_NAME, MASTER}
import org.apache.spark.sql.{DataFrame, SparkSession}

object WriteParquet {

  val spark: SparkSession = SparkSession.builder()
    .master(MASTER)
    .appName(APP_NAME)
    .getOrCreate()

  def write(file_type: String, df: DataFrame): Unit = {

    val filepath = "src/main/output/"

    if (file_type.equals("csv")) {
      df.coalesce(1).write.mode("overwrite").csv(filepath)
    }
    else if (file_type.equals("parquet")) {
      df.coalesce(1).write.mode("overwrite").parquet(filepath)
    }
    else {
      println("This file type is not supported.")
    }

  }

}
