package org.anish.spark.unused

import com.twitter.algebird.{CMS, CMSHasher, CMSHasherImplicits}
import org.anish.spark.skew.Utils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

import scala.math.{max => smax, min => smin}
//import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.util.Random

/**
  * Created by anish on 29/10/17.
  */
object UnderstandingSkewJoin {

  case class Prices(symbol: String, date: String, f1: Double, f2: Double, f3: Double, f4: Double, f5: Double)

  case class AggLookUps(symbol: String, date: String, open: Double, close: Double, low: Double, high: Double, volume: Double)

  val sparkSession = SparkSession.builder().master("local[8]").appName("SkewJoin").getOrCreate()

  def main(args: Array[String]): Unit = {
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

    val day_level_agg_lookup_data = sparkSession
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .schema(Encoders.product[AggLookUps].schema)
      .load("data/skewed_stock/someCalculatedLargeData_withdups")
      .as[AggLookUps]

    import CMSHasherImplicits._
    val leftPair = prices_withFeatures.map {
      p => (p.symbol + "_" + p.date, p) // The Algebird CMS hasher is currently defined only for primary types, so key is string
    }

    val rightPair = day_level_agg_lookup_data.map {
      a => (a.symbol + "_" + a.date, a)
    }


    mapPartitionsSkewJoin(leftPair, rightPair)
    mySkewJoin(leftPair, rightPair)


    /*val cms_l_bc = sparkSession.sparkContext.broadcast(leftPair.getCmsForKey())
    val cms_r_bc = sparkSession.sparkContext.broadcast(rightPair.getCmsForKey())


    val skewReplication = DefaultSkewReplication(1e-3)
    val numPartitions = leftPair.rdd.partitions.length // TODO use partitioner in rdd to find final number of partitions

    val replicationFactor: Double = 1e-3 // 1/rF number of records would be in 1 partition when replicated

    def getReplicationFactors(random: Random, replication: Int, otherReplication: Int): Seq[(Int, Int)] = {
      require(replication > 0 && otherReplication > 0, "replication must be positive")
      val rand = random.nextInt(otherReplication)
      (0 until replication).map(rep => (rand, rep))
    }


    val leftSalted = leftPair.mapPartitions { it =>
      val random = new Random(1)
      it.flatMap { kv =>

        val lc = cms_l_bc.value.frequency(kv._1).estimate
        val rc = cms_r_bc.value.frequency(kv._1).estimate
        val leftReplication = smax(smin((rc * replicationFactor).toInt, numPartitions), 1)
        val rightReplication = smax(smin((lc * replicationFactor).toInt, numPartitions), 1)



        //        println("k is " + kv._1 + s" and replications are $leftReplication, $rightReplication" +
        //          s" left estimate is ${cms_l_bc.value.frequency(kv._1).estimate}" +
        //          s" right estimate is ${cms_r_bc.value.frequency(kv._1).estimate}")
        getReplicationFactors(random, leftReplication, rightReplication).map(rl => ((kv._1, rl.swap), kv._2))
      }
    }

    val rightSalted = rightPair.mapPartitions { it =>
      val random = new Random(1)
      it.flatMap { kv =>
        val (leftReplication, rightReplication) = skewReplication.getReplications(
          cms_l_bc.value.frequency(kv._1).estimate,
          cms_r_bc.value.frequency(kv._1).estimate,
          numPartitions)


        getReplicationFactors(random, rightReplication, leftReplication).map(lr => ((kv._1, lr), kv._2))
      }
    }

    val joined = leftSalted.joinWith(rightSalted, leftSalted("_1") === rightSalted("_1"))
      .map { j => (j._1._2, j._2._2) }.toDF

    //    joined.printSchema()
    //
    joined.timedSaveToDisk("salted joined")*/

  }


  def mapPartitionsSkewJoin[K: TypeTag : Ordering : CMSHasher, V: TypeTag, W: TypeTag](leftPair: Dataset[(K, V)], rightPair: Dataset[(K, W)]) = {
    import Utils.dataset
    import Utils.pairdatasetcms
//    import CmsUtils._
    import sparkSession.implicits._


    val cms_l_bc = sparkSession.sparkContext.broadcast(leftPair.getCmsForKey())
    val cms_r_bc = sparkSession.sparkContext.broadcast(rightPair.getCmsForKey())


    val skewReplication = DefaultSkewReplication(1e-3)
    val numPartitions = leftPair.rdd.partitions.length // TODO use partitioner in rdd to find final number of partitions

    val replicationFactor: Double = 1e-3 // 1/rF number of records would be in 1 partition when replicated

    def getReplicationFactors(random: Random, replication: Int, otherReplication: Int): Seq[(Int, Int)] = {
      require(replication > 0 && otherReplication > 0, "replication must be positive")
      val rand = random.nextInt(otherReplication)
      (0 until replication).map(rep => (rand, rep))
    }


    val leftSalted = leftPair.mapPartitions { it =>
      val random = new Random(1)
      it.flatMap { kv =>

        val lc = cms_l_bc.value.frequency(kv._1).estimate
        val rc = cms_r_bc.value.frequency(kv._1).estimate
        val leftReplication = smax(smin((rc * replicationFactor).toInt, numPartitions), 1)
        val rightReplication = smax(smin((lc * replicationFactor).toInt, numPartitions), 1)



        //        println("k is " + kv._1 + s" and replications are $leftReplication, $rightReplication" +
        //          s" left estimate is ${cms_l_bc.value.frequency(kv._1).estimate}" +
        //          s" right estimate is ${cms_r_bc.value.frequency(kv._1).estimate}")
        getReplicationFactors(random, leftReplication, rightReplication).map(rl => ((kv._1, rl.swap), kv._2))
      }
    }

    val rightSalted = rightPair.mapPartitions { it =>
      val random = new Random(1)
      it.flatMap { kv =>
        val (leftReplication, rightReplication) = skewReplication.getReplications(
          cms_l_bc.value.frequency(kv._1).estimate,
          cms_r_bc.value.frequency(kv._1).estimate,
          numPartitions)


        getReplicationFactors(random, rightReplication, leftReplication).map(lr => ((kv._1, lr), kv._2))
      }
    }

    val joined = leftSalted.joinWith(rightSalted, leftSalted("_1") === rightSalted("_1"))
      .map { j => (j._1._2, j._2._2) }//.toDF

    //    joined.printSchema()
    //
    joined.timedSaveToDisk("salted joined")

  }

  def mySkewJoin[K : Ordering : CMSHasher: TypeTag, V, W](left: Dataset[(K, V)], right: Dataset[(K, W)]) = {

    val sparkSession = left.sparkSession
    import sparkSession.implicits._
    // TODO merge these two
    import Utils.dataset
    import Utils.pairdatasetcms

    val cms_l_bc = sparkSession.sparkContext.broadcast(left.getCmsForKey())
    val cms_r_bc = sparkSession.sparkContext.broadcast(right.getCmsForKey())

    val lRandom_bc = sparkSession.sparkContext.broadcast(new Random(1))
    val rRandom_bc = sparkSession.sparkContext.broadcast(new Random(2))

    val skewReplication = DefaultSkewReplication()
    val numPartitions = left.rdd.partitions.length // TODO use partitioner in rdd to find final number of partitions

    val replicationFactor: Double = 1e-3 // 1/rF number of records would be in 1 partition when replicated


    // Fragment Left DF
    val fragmentLeftUdf = udf { id: K =>
      val random = lRandom_bc.value
      val lc = cms_l_bc.value.frequency(id).estimate
      //      val rc = cms_r_bc.value.frequency(id).estimate
      //      val leftReplication = max(min((rc * replicationFactor).toInt, numPartitions), 1)
      val rightReplication = smax(smin((lc * replicationFactor).toInt, numPartitions), 1) // TODO put numPartitions and cms and random in one Monad and broadcast
      // Based on left count find right replication => right should be replicated if left frequency is high

      // based on right replication, generate fragment for left
      random.nextInt(rightReplication)
    }

    val fragmentRightUdf = udf { id: K =>
      val random = rRandom_bc.value
      //      val lc = cms_l_bc.value.frequency(id).estimate
      val rc = cms_r_bc.value.frequency(id).estimate
      val leftReplication = smax(smin((rc * replicationFactor).toInt, numPartitions), 1)
      //      val rightReplication = max(min((lc * replicationFactor).toInt, numPartitions), 1) // TODO put numPartitions and cms and random in one Monad and broadcast
      // Based on right count find left replication => left should be replicated if right frequency is high

      // based on left replication, generate fragment for right
      random.nextInt(leftReplication)
    }


    // replication using explode. This will produce an array of indexes, use this to while exploding
    val replicateIndexesLeftUdf = udf { id: K => // Generate an array of replica indexes
      val rc = cms_r_bc.value.frequency(id).estimate
      val leftReplication = smax(smin((rc * replicationFactor).toInt, numPartitions), 1)

      0 until leftReplication toArray
    }

    val replicateIndexesRightUdf = udf { id: K => // Generate an array of replica indexes
      val lc = cms_l_bc.value.frequency(id).estimate
      val rightReplication = smax(smin((lc * replicationFactor).toInt, numPartitions), 1)

      0 until rightReplication toArray
    }


    val leftSalted = left.withColumn("salt1", fragmentLeftUdf('_1)) // _1 should be K
      .withColumn("salt2", explode(replicateIndexesLeftUdf('_1)))
      .withColumnRenamed("_2", "left_2")


    val rightSalted = right.withColumn("salt2", fragmentRightUdf('_1))
      .withColumn("salt1", explode(replicateIndexesRightUdf('_1)))
      .withColumnRenamed("_2", "right_2")

    val joined = leftSalted.join(rightSalted, Array("_1", "salt1", "salt2"))
    println("Join using DF functions")
    joined.printSchema()

    joined.cache.timedSaveToDisk("fr cms join")
    joined.showPartitionStats()

    //    import sparkSession.implicits._
    //    val left_fragmented_replicated = left.flatMap { kv =>
    //      val random = lRandom_bc.value
    //      val lc = cms_l_bc.value.frequency(kv._1).estimate
    //      val rc = cms_r_bc.value.frequency(kv._1).estimate
    //      val leftReplication = smax(smin((rc * replicationFactor).toInt, numPartitions), 1)
    //      val rightReplication = smax(smin((lc * replicationFactor).toInt, numPartitions), 1) // TODO put numPartitions and cms and random in one Monad and broadcast
    //      // Based on left count find right replication => right should be replicated if left frequency is high
    //      (0 until leftReplication).map(lr => ((lr, random.nextInt(rightReplication)), kv))
    //    }


    //    def getReplicationFactors(random: Random, replication: Int, otherReplication: Int): Seq[(Int, Int)] = {
    //      require(replication > 0 && otherReplication > 0, "replication must be positive")
    //      val rand = random.nextInt(otherReplication)
    //      (0 until replication).map(rep => (rand, rep))
    //    }
    //
    //
    //    val leftSalted = left.mapPartitions { it =>
    //      val random = new Random(1)
    //      it.flatMap { kv =>
    //
    //        val lc = cms_l_bc.value.frequency(kv._1).estimate
    //        val rc = cms_r_bc.value.frequency(kv._1).estimate
    //        val leftReplication = max(min((rc * replicationFactor).toInt, numPartitions), 1)
    //        val rightReplication = max(min((lc * replicationFactor).toInt, numPartitions), 1)
    //
    //
    //
    //        //        println("k is " + kv._1 + s" and replications are $leftReplication, $rightReplication" +
    //        //          s" left estimate is ${cms_l_bc.value.frequency(kv._1).estimate}" +
    //        //          s" right estimate is ${cms_r_bc.value.frequency(kv._1).estimate}")
    //        getReplicationFactors(random, leftReplication, rightReplication).map(rl => ((kv._1, rl.swap), kv._2))
    //      }
    //    }
    //
    //    val rightSalted = right.mapPartitions { it =>
    //      val random = new Random(1)
    //      it.flatMap { kv =>
    //        val (leftReplication, rightReplication) = skewReplication.getReplications(
    //          cms_l_bc.value.frequency(kv._1).estimate,
    //          cms_r_bc.value.frequency(kv._1).estimate,
    //          numPartitions)
    //
    //
    //
    //
    //        getReplicationFactors(random, rightReplication, leftReplication).map(lr => ((kv._1, lr), kv._2))
    //      }
    //    }
    //
    //    val joined = leftSalted.joinWith(rightSalted, leftSalted("_1") === rightSalted("_1"))
    //      .map { j => (j._1._2, j._2._2) }.toDF
    //
    //    joined.printSchema()
    //
    //    joined.timedSaveToDisk("salted joined")


  }


}

object CmsUtils {

  implicit class Utils[K: Ordering : CMSHasher, V](dataset: Dataset[(K, V)]) {

    /**
      * Defined on a Dataset of (K, V), creates and returns a CMS for keys of type K
      *
      * @param eps
      * @param delta
      * @param seed
      * @return
      */
    def getCmsForKey(eps: Double = 0.005, delta: Double = 1e-8, seed: Int = 1): CMS[K] = {
      val cmsMonoid = CMS.monoid[K](eps, delta, seed)
      dataset.rdd.map(kv => cmsMonoid.create(kv._1)).reduce(cmsMonoid.plus(_, _))
    }
  }

}

trait SkewReplication extends Serializable {
  def getReplications(leftCount: Long, rightCount: Long, numPartitions: Int): (Int, Int)
}

case class DefaultSkewReplication(replicationFactor: Double = 1e-2) extends SkewReplication {
  override def getReplications(leftCount: Long, rightCount: Long, numPartitions: Int): (Int, Int) = (
    smax(smin((rightCount * replicationFactor).toInt, numPartitions), 1),
    smax(smin((leftCount * replicationFactor).toInt, numPartitions), 1)
    )
}

