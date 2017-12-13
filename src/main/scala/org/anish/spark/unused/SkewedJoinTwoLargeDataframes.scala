package org.anish.spark.unused

import org.anish.spark.skew.Utils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

import scala.io.StdIn

/**
  * Created by anish on 19/10/17.
  */
object SkewedJoinTwoLargeDataframes {

  case class Prices(symbol: String, date: String, f1: Double, f2: Double, f3: Double, f4: Double, f5: Double)

  case class AggLookUps(symbol: String, date: String, open: Double, close: Double, low: Double, high: Double, volume: Double)

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().master("local[8]").appName("SkewJoin").getOrCreate()
    import Utils.dataset
    import sparkSession.implicits._

    sparkSession.conf.set("spark.sql.shuffle.partitions", "16")


    val prices_withFeatures = sparkSession
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .schema(Encoders.product[Prices].schema) // This is to to allow conversion from string in csv to double in case class
      .load("data/skewed_stock/prices_withFeatures")
      // This data is partitioned by symbol
      .as[Prices]
      .cache

    //
    //      .repartition('symbol) // Num partitions would be spark.sql.shuffle.partitions
    println("Stats for Prices")
    prices_withFeatures.showPartitionStats()




    // Visualize prices data
    //    val prices_withFeatures_agg = prices_withFeatures
    //      .groupBy('symbol)
    //      .agg(count("*") as "symbolFreq")
    //    Vegas("prices for each symbol")
    //      .withDataFrame(prices_withFeatures_agg, 150)
    //      .encodeX("symbol", Nom)
    //      .encodeY("symbolFreq", Quant)
    //      .mark(Bar)
    //      .show


    // Some other large data (Look Up) = Key is date, symbol
    val day_level_agg_lookup_data = sparkSession
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
//      .load("data/skewed_stock/someCalculatedLargeData")
          .load("data/skewed_stock/someCalculatedLargeData_withdups")

    println("Stats for day_level_agg_lookup_data")
    day_level_agg_lookup_data.showPartitionStats()


    // The following always gives 1, so it us useless
    //    // Visualize 2nd Data Frame
    //    val day_level_agg_histo = day_level_agg_lookup_data
    //      .withColumn("symbol_date", concat('symbol, lit("_"), 'date)) // because we can have only 1 col in x-axis
    //      .groupBy('symbol_date)
    //      .agg(count("*") as "cnt")
    //    Vegas("Symbol With Date Counts - 2nd DF")
    //      .withDataFrame(day_level_agg_histo, 150)
    //      .encodeX("symbol_date", Nom)
    //      .encodeY("cnt", Quant)
    //      .mark(Bar)
    //      .show


    sparkSession.sparkContext.setJobGroup("1", "Simple Join of two large data sets")
    val joinedDf = prices_withFeatures.join(day_level_agg_lookup_data, Array("symbol", "date"))


    joinedDf.cache.timedSaveToDisk("joining large and large DF")
    println("Stats for Joined data")
    sparkSession.sparkContext.clearJobGroup()

    joinedDf.showPartitionStats()
    //    joinedDf.explain(true)

    sparkSession.sparkContext.setJobGroup("3", "Fragment Replicate Join")
    val frJoined = fragmentReplicateJoin(prices_withFeatures, day_level_agg_lookup_data, 4, Array("symbol", "date"))
    frJoined.cache().timedSaveToDisk("Fragment Replicated Join")
    sparkSession.sparkContext.clearJobGroup()

    frJoined.showPartitionStats()
    //    joinedDf.ensureDatasetEquals(frJoined)

    // _withdups use this for 2nd data, for better results
    sparkSession.sparkContext.setJobGroup("3", "Fragment Replicate Join with inequal replication factors")
    val frJoinWithDifferentReplicationFactors = fragmentReplicateJoin(prices_withFeatures, day_level_agg_lookup_data,
      Array("symbol", "date"), 2, 4)
    frJoinWithDifferentReplicationFactors.timedSaveToDisk("Inequal replication")
    sparkSession.sparkContext.clearJobGroup()
    frJoinWithDifferentReplicationFactors.showPartitionStats()


    // Increase partitions in one DF to remove skew = use salt derivable from the key TODO check tresata

    // Add Salt to prices_withFeatures


    println("Press return to Exit") // Keep Spark UI active (only for local machine)
    StdIn.readLine()
    sparkSession.stop()
  }

  // http://www.exploredatabase.com/2014/03/fragment-and-replicate-join-parallel.html
  // Replicate each record of replicate DS by salting factor times
  def fragmentReplicateJoin[V, W](fragment: Dataset[V], replicate: Dataset[W], saltingFactor: Int, joinCol: Array[String]): Dataset[Row] = {
    require(saltingFactor > 0, "Salting factor should be positive")
    val sparkSession = replicate.sparkSession
    import sparkSession.implicits._
    val replicated = sparkSession.sparkContext.parallelize(0 until saltingFactor).toDF("salt") // salt is from 0 to saltingFactor - 1
      .crossJoin(replicate)

    val fragmented = fragment.withColumn("salt", rand(100) * saltingFactor cast IntegerType)

    fragmented.join(replicated, joinCol :+ "salt")
      .drop(fragmented("salt"))
      .drop(replicated("salt"))
  }

  // Separate replications for each side of the data frame
  //
  def fragmentReplicateJoin[V, W](left: Dataset[V], right: Dataset[W], joinCol: Array[String], leftReplicationFactor: Int, rightReplicationFactor: Int) = {
    require(leftReplicationFactor > 0 && rightReplicationFactor > 0, "Replication factors should be greater than 0")

    val sparkSession = left.sparkSession
    import sparkSession.implicits._

    val leftFragmented = left.withColumn("frag", rand(100) * rightReplicationFactor cast IntegerType)
    val leftReplicated = sparkSession.sparkContext.parallelize(0 until leftReplicationFactor).toDF("lrepl").crossJoin(leftFragmented)
      .withColumnRenamed("frag", "salt1")
      .withColumnRenamed("lrepl", "salt2")


    val rightFragmented = right.withColumn("frag", rand(100) * leftReplicationFactor cast IntegerType)
    val rightReplicated = sparkSession.sparkContext.parallelize(0 until rightReplicationFactor).toDF("rrepl").crossJoin(rightFragmented)
      .withColumnRenamed("frag", "salt2") // This is opposite of left
      .withColumnRenamed("rrepl", "salt1")

    leftReplicated.join(rightReplicated, joinCol :+ "salt1" :+ "salt2")
      .drop(leftReplicated("salt1"))
      .drop(leftReplicated("salt2"))
      .drop(rightReplicated("salt1"))
      .drop(rightReplicated("salt2"))
  }

}


/*

FR Join
4
Stats for Prices
Total Partitions -> 75
Total Records -> 3851264
Percentiles -> Min 	| 25th 	| 50th 	| 75th 	| Max
Percentiles -> 2004 	| 7504 	| 7761 	| 15280 	| 1875000
Stats for day_level_agg_lookup_data
Total Partitions -> 10
Total Records -> 515553
Percentiles -> Min 	| 25th 	| 50th 	| 75th 	| Max
Percentiles -> 7264 	| 56384 	| 56384 	| 56384 	| 58146
Elapsed time for joining large and large DF : 21230898268 ns or 21230.898268 ms
Stats for Joined data
Total Partitions -> 16
Total Records -> 3851264
Percentiles -> Min 	| 25th 	| 50th 	| 75th 	| Max
Percentiles -> 52851 	| 53132 	| 53199 	| 53618 	| 3052762
Elapsed time for Fragment Replicated Join : 14816030080 ns or 14816.03008 ms
Total Partitions -> 16
Total Records -> 3851264
Percentiles -> Min 	| 25th 	| 50th 	| 75th 	| Max
Percentiles -> 52841 	| 53181 	| 53271 	| 802773 	| 803677
Press return to Exit


 */

/*

FR Inequal
Stats for Prices
[Stage 2:========================================================>(74 + 1) / 75]Total Partitions -> 75
Total Records -> 3851264
Percentiles -> Min 	| 25th 	| 50th 	| 75th 	| Max
Percentiles -> 2004 	| 7504 	| 7761 	| 15280 	| 1875000
Stats for day_level_agg_lookup_data
[Stage 7:====================================================>     (9 + 1) / 10]Total Partitions -> 10
                                                                                Total Records -> 2515553
Percentiles -> Min 	| 25th 	| 50th 	| 75th 	| Max
Percentiles -> 38004 	| 56384 	| 56384 	| 56384 	| 2026430
[Stage 10:>                                                        (0 + 8) / 16]SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
[Stage 13:====================================================>   (15 + 1) / 16]Elapsed time for joining large and large DF : 54663810549 ns or 54663.810549 ms
Stats for Joined data
[Stage 14:=========>      (47 + 8) / 80][Stage 15:>                (0 + 0) / 75]Total Partitions -> 16
Total Records -> 9851264
Percentiles -> Min 	| 25th 	| 50th 	| 75th 	| Max
Percentiles -> 52851 	| 53132 	| 53199 	| 53618 	| 9052762
Elapsed time for Fragment Replicated Join : 55267782320 ns or 55267.78232 ms
[Stage 19:====================================================>   (15 + 1) / 16]Total Partitions -> 16
                                                                                Total Records -> 9851264
Percentiles -> Min 	| 25th 	| 50th 	| 75th 	| Max
Percentiles -> 52841 	| 53226 	| 53408 	| 803482 	| 4053174
[Stage 22:===================================>                    (10 + 6) / 16]Elapsed time for Inequal replication : 47737221309 ns or 47737.221309 ms
Total Partitions -> 16
Total Records -> 9851264
Percentiles -> Min 	| 25th 	| 50th 	| 75th 	| Max
Percentiles -> 52775 	| 53214 	| 53525 	| 1053583 	| 2054270
Press return to Exit

 */