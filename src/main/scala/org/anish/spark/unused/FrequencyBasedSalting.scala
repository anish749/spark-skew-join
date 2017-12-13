package org.anish.spark.unused

import com.twitter.algebird.{CMS, CMSHasher}
import org.anish.spark.skew._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Dataset, Encoder}

import scala.math.{max, min}
import scala.reflect.runtime.universe._
import scala.util.Random

/**
  * Created by anish on 31/10/17.
  */
private[unused] object FrequencyBasedSalting {

  def salt[K: Ordering : CMSHasher : TypeTag, V: TypeTag, W: TypeTag]
  (left: Dataset[(K, V)], right: Dataset[(K, W)], skewJoinConfig: SkewJoinConf)
  (lt_encoder: Encoder[(K, V)], rt_encoder: Encoder[(K, W)])
  : (Dataset[((K, (Int, Int)), V)], Dataset[((K, (Int, Int)), W)]) = {
    val sparkContext = left.sparkSession.sparkContext
    import left.sparkSession.implicits._
    import skewJoinConfig._
    val cms_l_bc = sparkContext.broadcast(left.getCmsForKey(CMSeps, CMSdelta, CMSseed))
    val cms_r_bc = sparkContext.broadcast(right.getCmsForKey(CMSeps, CMSdelta, CMSseed))

    skewType match {
      case CrossSkew =>
      case LeftSkew => //leftReplication = 1
      case RightSkew => // rightReplication = 1
    }
    val skewReplication = DefaultSkewReplication(1e-3)

    val numPartitions = left.rdd.partitions.length // TODO use partitioner in rdd to find final number of partitions

    val replicationFactor: Double = 1e-3 // 1/rF number of records would be in 1 partition when replicated

    def getReplicationFactors(random: Random, replication: Int, otherReplication: Int): Seq[(Int, Int)] = {
      require(replication > 0 && otherReplication > 0, "replication must be positive")
      val rand = random.nextInt(otherReplication)
      (0 until replication).map(rep => (rand, rep))
    }


    //    val targs = tag_k.tpe match {
    //      case TypeRef(_, _, args) => args
    //    }

    println( ) // if(typeOf[V] == "org.apache.spark.sql.Row") ExpressionEncoder[Row] else
    implicit val k_encoder = ExpressionEncoder[K]
//    implicit val v_encoder = ExpressionEncoder[V] // Need to be passed by caller
//    implicit val w_encoder = ExpressionEncoder[W]

    val leftSalted = left.mapPartitions { it =>
      val random = new Random(1)
      it.flatMap { kv =>
        val lc = cms_l_bc.value.frequency(kv._1).estimate
        val rc = cms_r_bc.value.frequency(kv._1).estimate
        val leftReplication = max(min((rc * replicationFactor).toInt, numPartitions), 1)
        val rightReplication = max(min((lc * replicationFactor).toInt, numPartitions), 1)
        getReplicationFactors(random, leftReplication, rightReplication).map(rl => ((kv._1, rl.swap), kv._2))
      }
    } //(lt_encoder)

    val rightSalted = right.mapPartitions { it =>
      val random = new Random(1)
      it.flatMap { kv =>
        val (leftReplication, rightReplication) = skewReplication.getReplications(
          cms_l_bc.value.frequency(kv._1).estimate,
          cms_r_bc.value.frequency(kv._1).estimate,
          numPartitions)
        getReplicationFactors(random, rightReplication, leftReplication).map(lr => ((kv._1, lr), kv._2))
      }
    }
    (leftSalted, rightSalted)

    //    val joined = leftSalted.joinWith(rightSalted, leftSalted("_1") === rightSalted("_1"))
    //      .map { j => (j._1._2, j._2._2) }.toDF
  }


  implicit class CmsUtils[K: Ordering : CMSHasher, V](dataset: Dataset[(K, V)]) {
    /**
      * Defined on a Dataset of (K, V), creates and returns a CMS for keys of type K
      *
      * @param eps
      * @param delta
      * @param seed
      * @return
      */
    def getCmsForKey(eps: Double, delta: Double, seed: Int): CMS[K] = {
      val cmsMonoid = CMS.monoid[K](eps, delta, seed)
      dataset.rdd.map(kv => cmsMonoid.create(kv._1)).reduce(cmsMonoid.plus(_, _))
    }
  }


}

