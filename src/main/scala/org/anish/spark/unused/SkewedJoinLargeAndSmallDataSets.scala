package org.anish.spark.unused

import org.anish.spark.skew.Utils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

import scala.io.StdIn

/**
  * Created by anish on 22/10/17.
  */
object SkewedJoinLargeAndSmallDataSets {

  case class LookUps(id: Long, symbol: String, company: String)

  case class SkewedFact(id: Long, some_value: Long)

  case class SaltedFact(id: Long, salt: Int, some_value: Long)

  case class SaltedLookUp(id: Long, salt: Int, symbol: String, company: String)

  case class DenormalizedData(id: Long, some_value: Long, symbol: String, company: String)

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().master("local[8]").appName("SkewJoin").getOrCreate()
    import Utils.dataset
    import sparkSession.implicits._

    sparkSession.sparkContext.setJobGroup("1", "Show Partition Stats on Dim Table")
    // Read a dimension table. This has got some data with we would using as look up / reference
    val dimTable = sparkSession
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("/Users/anish/git/spark-210trial/data/stock_symbol_company_mapping.csv")
      .coalesce(1)
      .withColumn("id", monotonically_increasing_id()) // Add an Id Column

    println("Dim Table Stats")
    dimTable.showPartitionStats()

    sparkSession.sparkContext.setJobGroup("2", "Show Partition Stats on Skewed Large Table")
    // Create skewed data
    val numPartitions = 24
    sparkSession.conf.set("spark.sql.shuffle.partitions", numPartitions)
    val skewedData = sparkSession
      .sparkContext
      .parallelize[Int](1 to numPartitions, numPartitions) // Explicitly set number of partitions, 1 record per partition
      .mapPartitionsWithIndex { (index, _) =>
      Stream.continually(index).toIterator.zip((1L to math.pow(2, index).toLong).iterator) // Create 2^i records on partition i
    }.toDF("id", "some_value") // Here the id references the id in the dim table.

    println("Skewed large data Stats")
    skewedData.showPartitionStats()


    // Clear cache after each stage
    // Because we want to calculate the time taken for the actual join
    skewedData.unpersist(true)
    dimTable.unpersist(true)

    sparkSession.sparkContext.setJobGroup("3", "Simple Join")
    // Do the actual join - de-normalization
    val joinedData = skewedData.join(dimTable, "id")
    joinedData.timedSaveToDisk("simple join on two data frames (large join small)")
    println("Simple join partition stats")
    joinedData.showPartitionStats()

    skewedData.unpersist(true)
    dimTable.unpersist(true)

    sparkSession.sparkContext.setJobGroup("4", "Broadcast Join using DataSet")
    // Broadcast join DF
    val broadcastJoined = skewedData.join(broadcast(dimTable.as("dimension")), "id")

    broadcastJoined.timedSaveToDisk("Broadcast join using Dataset")
    println("broadcast join partition stats")
    broadcastJoined.showPartitionStats()

    sparkSession.sparkContext.setJobGroup("5", "Broadcast Join RDD")
    val dim = dimTable.as[LookUps]
    val skewedRDD = skewedData.as[SkewedFact].rdd

    val dimBroadcasted = sparkSession.sparkContext.broadcast(
      dim.collect()
        .map(lk => lk.id -> lk)
        .toMap
    )

    val broadCastJoinUsingRDD = skewedRDD.map { fact => // The actual Map Side Hash Join
      val lookedUpValue = dimBroadcasted.value(fact.id)
      DenormalizedData(fact.id, fact.some_value, lookedUpValue.symbol, lookedUpValue.company)
    }.toDS()

    broadCastJoinUsingRDD.timedSaveToDisk("Broadcast join using RDD")
    println("broadcast join using rdd partition stats")
    broadCastJoinUsingRDD.showPartitionStats()
    broadCastJoinUsingRDD.unpersist(true)


    sparkSession.sparkContext.setJobGroup("6", "Salted Join Preps")
    val saltingFactor = 8
    val saltedLargeTableRDD = skewedData
      .withColumn("salt", rand(100) * saltingFactor cast IntegerType) // 100 is seed, generates a random number in the range [0, 1 * saltingFactor)
      .as[SaltedFact]
      .rdd // Switch to RDD API now, because we want custom partitioner, and Datasets are not suited for these low level operations
      .cache


    sparkSession.sparkContext.setJobGroup("7", "Broadcast Salted Join (Fragment and Replicate Join) - Map Side")
    // http://www.exploredatabase.com/2014/03/fragment-and-replicate-join-parallel.html
    val denormalizedDS_NoIsolationJoin = saltedLargeTableRDD.keyBy(sf => (sf.id, sf.salt))
      .partitionBy(new SaltedIsolatedPartitioner(numPartitions, 0, // This means No Isolation when it comes to salting. Everything is salted
        saltingFactor))
      .mapValues { fact => // The actual Map Side Hash Join
        val lookedUpValue = dimBroadcasted.value(fact.id)
        DenormalizedData(fact.id, fact.some_value, lookedUpValue.symbol, lookedUpValue.company)
      }
      .map(_._2) // We no longer need the key
      .toDS()

    denormalizedDS_NoIsolationJoin.timedSaveToDisk("Broadcast Salted Join - Map side")
    println("broadcast salted join using rdd partition stats")
    denormalizedDS_NoIsolationJoin.showPartitionStats()
    denormalizedDS_NoIsolationJoin.unpersist(true)

    sparkSession.sparkContext.setJobGroup("8", "Broadcast Isolated Salted Join (Map side)")
    val denormalizedDS_IsolatedJoined = saltedLargeTableRDD.keyBy(sf => (sf.id, sf.salt))
      .partitionBy(new SaltedIsolatedPartitioner(numPartitions, 18, // Means keep partitions 0-17 intact // 0 means salting all partitions
        saltingFactor)) // Divide partitions from 18 or more into salting factor
      .mapValues { fact => // The actual Map Side Hash Join
      val lookedUpValue = dimBroadcasted.value(fact.id)
      DenormalizedData(fact.id, fact.some_value, lookedUpValue.symbol, lookedUpValue.company)
    }
      .map(_._2) // We no longer need the key
      .toDS()

    denormalizedDS_IsolatedJoined.timedSaveToDisk("Broadcast Isolated Salted Join (Map side)")
    println("broadcast isolated salted join using rdd partition stats")
    denormalizedDS_IsolatedJoined.showPartitionStats()
    denormalizedDS_IsolatedJoined.unpersist(true)

    saltedLargeTableRDD.unpersist(true)

    // TODO check for equality of outputs

    println("Press return to Exit") // Keep Spark UI active (only for local machine)
    StdIn.readLine()
    sparkSession.stop()
  }

}

/*

Dim Table Stats
Total Partitions -> 1
Total Records -> 1262
Percentiles -> Min 	| 25th 	| 50th 	| 75th 	| Max
Percentiles -> 1262 	| 1262 	| 1262 	| 1262 	| 1262
Skewed large data Stats
Total Partitions -> 24
Total Records -> 16777215
Percentiles -> Min 	| 25th 	| 50th 	| 75th 	| Max
Percentiles -> 1 	| 64 	| 4096 	| 262144 	| 8388608

Elapsed time for simple join on two data frames (large join small) : 8189569945 ns or 8189.569945 ms
Simple join partition stats
Total Partitions -> 24
Total Records -> 16777215
Percentiles -> Min 	| 25th 	| 50th 	| 75th 	| Max
Percentiles -> 1 	| 64 	| 4096 	| 262144 	| 8388608
Elapsed time for Broadcast join using Dataset : 6177646240 ns or 6177.64624 ms
broadcast join partition stats
Total Partitions -> 24
Total Records -> 16777215
Percentiles -> Min 	| 25th 	| 50th 	| 75th 	| Max
Percentiles -> 1 	| 64 	| 4096 	| 262144 	| 8388608
Elapsed time for Broadcast join using RDD : 7892236689 ns or 7892.236689 ms
broadcast join using rdd partition stats
Total Partitions -> 24
Total Records -> 16777215
Percentiles -> Min 	| 25th 	| 50th 	| 75th 	| Max
Percentiles -> 1 	| 64 	| 4096 	| 262144 	| 8388608
Elapsed time for Broadcast Salted Join : 24767244769 ns or 24767.244769 ms
broadcast salted join using rdd partition stats
Total Partitions -> 192
Total Records -> 16777215
Percentiles -> Min 	| 25th 	| 50th 	| 75th 	| Max
Percentiles -> 0 	| 5 	| 492 	| 32569 	| 1050236
Elapsed time for Broadcast Isolated Salted Join (Map side) : 14227477338 ns or 14227.477338 ms
broadcast isolated salted join using rdd partition stats
Total Partitions -> 66
Total Records -> 16777215
Percentiles -> Min 	| 25th 	| 50th 	| 75th 	| Max
Percentiles -> 1 	| 32687 	| 130564 	| 262753 	| 1050236
Press return to Exit



https://docs.databricks.com/spark/latest/spark-sql/skew-join.html
*/