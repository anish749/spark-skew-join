package org.anish.spark.unused

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by anish on 17/10/17.
  */
object JoinBigAndSmall {

  // TODO try with wikipedia hits data set
  // TODO OR gutenberg or whatever


  def main(args: Array[String]): Unit = {

    val numPartitions = 24

    val sparkSession = SparkSession.builder().master("local[8]").appName("SkewJoin").getOrCreate()
    import sparkSession.implicits._

    val something = sparkSession.sparkContext.parallelize[Int](1 to numPartitions, numPartitions) // Explicitly set number of partitions, 1 record per partition
      .toDS.cache

    println(s"Number of partitions is ${something.rdd.getNumPartitions}")

    val skewedData = something.rdd.mapPartitionsWithIndex { (index, _) =>
      Stream.continually(index).toIterator.zip((1L to math.pow(2, index).toLong).iterator)
    }.toDF("id", "some_value")
      .cache

    //    skewedData.cache.show

    //    skewedData.groupBy("id")
    //      .agg(count("some_value"))
    //      .show()

    //    val elementsPerPartition = skewedData.rdd.mapPartitionsWithIndex { (index, iter) =>
    //      List((index, iter.size)).iterator
    //    }.collect.toList
    //    println(elementsPerPartition)


    val dimTable = sparkSession
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("/Users/anish/git/spark-210trial/data/stock_symbol_company_mapping.csv")
      .coalesce(1)
      .withColumn("id", monotonically_increasing_id())
      .cache

    sparkSession.conf.set("spark.sql.shuffle.partitions", numPartitions)

    time("simple join on two cached data frames") {
      skewedData.join(dimTable, "id")
        .write
        .mode(SaveMode.Overwrite)
        .save("data/tmpLargeData/")
      //        .show(20000000)
    }


    // Broadcast join DF

    val broadcastedDimension = broadcast(dimTable.as("dimension"))
    val broadcastJoined = skewedData.join(broadcast(broadcastedDimension), "id")

    time("broadcast join of two cached data frames") {
      broadcastJoined.write
        .mode(SaveMode.Overwrite)
        .save("data/tmpLargeData/")
      //        .show(20000000)
    }





    // TODO verify if all the outputs are matching





    Thread.sleep(1000 * 60 * 60) // Wait for an hour - keeping the Spark Context Alive
  }

  // I know this is silly for Spark, better check the Spark UI and see the time required for each stage
  def time[R](blockName: String)(block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val timeElapsedNano = System.nanoTime() - t0
    println(s"Elapsed time for $blockName : $timeElapsedNano ns or ${
      timeElapsedNano / 1e6
    } ms")
    result
  }
}
