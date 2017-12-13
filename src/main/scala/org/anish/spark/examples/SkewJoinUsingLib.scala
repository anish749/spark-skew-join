package org.anish.spark.examples

import com.twitter.algebird.CMSHasherImplicits
import org.anish.spark.skew.{LeftSkew, SkewJoinConf, Utils}
import org.apache.spark.sql.{Encoders, SparkSession}

/**
  * Created by anish on 31/10/17.
  */
case class Prices(symbol: String, date: String, f1: Double, f2: Double, f3: Double, f4: Double, f5: Double)

case class AggLookUps(symbol: String, date: String, open: Double, close: Double, low: Double, high: Double, volume: Double)

object SkewJoinUsingLib {
  val sparkSession = SparkSession.builder().master("local[8]").appName("SkewJoin").getOrCreate()

  import sparkSession.implicits._
  import Utils.dataset

  sparkSession.conf.set("spark.sql.shuffle.partitions", "16")

  def main(args: Array[String]): Unit = {
    ds()
    //    df()
  }

  // With DataSets
  def ds(): Unit = {

    val prices_withFeatures = sparkSession
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .schema(Encoders.product[Prices].schema) // This is to to allow conversion from string in csv to double in case class
      .load("data/skewed_stock/prices_withFeatures")
      // This data is partitioned by symbol
      .as[Prices]

    val day_level_agg_lookup_data = sparkSession
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .schema(Encoders.product[AggLookUps].schema)
      //      .load("data/skewed_stock/someCalculatedLargeData_withdups")
      .load("data/skewed_stock/someCalculatedLargeData")
      .as[AggLookUps]

    val leftPair = prices_withFeatures.map {
      p => (p.symbol + "_" + p.date, p) // The Algebird CMS hasher is currently defined only for selected types, so key is string
    }
    val rightPair = day_level_agg_lookup_data.map {
      a => (a.symbol + "_" + a.date, a)
    }

    import org.anish.spark.skew.dsimplicits._
    import CMSHasherImplicits._
    // Because K needs to have an implicit CMS Hasher defined.
    leftPair.printSchema()
    leftPair.select("_1").show()
    val cndn = leftPair("") === rightPair("_1")
    val joined = leftPair.skewJoin(rightPair, cndn)

    joined.printSchema()
    joined.timedSaveToDisk("Skew Join using libs")
    joined.showPartitionStats()
    joined.show()
  }

  // With Data Frames
  def df(): Unit = {

    val prices_withFeatures = sparkSession
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("data/skewed_stock/prices_withFeatures")
    // This data is partitioned by symbol

    val day_level_agg_lookup_data = sparkSession
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      //      .load("data/skewed_stock/someCalculatedLargeData_withdups")
      .load("data/skewed_stock/someCalculatedLargeData")

    //    normal join
    val normalJoin = prices_withFeatures.join(day_level_agg_lookup_data, Array("symbol", "date"), "inner")

    normalJoin.timedSaveToDisk("Normal DF Join")
    normalJoin.printSchema()
    normalJoin.showPartitionStats()


    import org.anish.spark.skew.dfimplicits._
    val skewJoined = prices_withFeatures.skewJoin(day_level_agg_lookup_data, Array("symbol", "date"), joinType = "inner", SkewJoinConf(skewType = LeftSkew))

    skewJoined.timedSaveToDisk("Skew Join using libs")
    skewJoined.printSchema()
    skewJoined.showPartitionStats()

  }

}
