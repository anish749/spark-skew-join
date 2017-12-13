package org.anish.spark.skew

import com.twitter.algebird.{CMSHasher, CMSHasherImplicits}
import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.sql.{Column, Dataset, Encoder, _}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
//import org.apache.spark.sql.functions._

import scala.math.{max, min}
import scala.reflect.runtime.universe._
import scala.util.Random

/**
  * Created by anish on 01/11/17.
  */
object dsimplicits {
  private final val JOIN_KEY = "_join_key_nbRcsSD1FW"
  private final val SALT_1 = "_salt_c3O4hPezNs_1"
  private final val SALT_2 = "_salt_bpyWIjwfIy_2"
  private final val SALTS = Seq(SALT_1, SALT_2)

  implicit class SkewJoinOnDataset[K: Ordering : CMSHasher : TypeTag : Encoder, V: TypeTag : Encoder](left: Dataset[(K, V)]) {

    def test()={
      left.show()
    }

    private def saltingForSkewJoin[W: TypeTag : Encoder](lt: Dataset[(K, V)],
                                                         rt: Dataset[(K, W)],
                                                         skewJoinConfig: SkewJoinConf)
    : (Dataset[(K, (Int, Int), V)], Dataset[(K, (Int, Int), W)]) = {

      val sparkContext = lt.sparkSession.sparkContext
      import lt.sparkSession.implicits._
      import skewJoinConfig._
      import Utils.datasetcms
      import CMSHasherImplicits._

      val cms_l_bc = sparkContext.broadcast(lt.map(_._1).getCMS(CMSeps, CMSdelta, CMSseed))
      val cms_r_bc = sparkContext.broadcast(rt.map(_._1).getCMS(CMSeps, CMSdelta, CMSseed))


      val numPartitions = defaultPartitioner(lt.rdd, rt.rdd).numPartitions

      // val replicationFactor: Double = 1e-3 // 1/rF number of records would be in 1 partition when replicated

      def getReplicationFactors(random: Random, replication: Int, otherReplication: Int): Seq[(Int, Int)] = {
        require(replication > 0 && otherReplication > 0, "replication must be positive")
        val rand = random.nextInt(otherReplication)
        (0 until replication).map(rep => (rand, rep))
      }


      val leftSalted = lt.mapPartitions { it =>
        val random = new Random(1) // using seed. - this is very important, as this would give the same sequence when this is rerun in case of failures (speculative execution)
        it.flatMap { kv =>
          val lc = cms_l_bc.value.frequency(kv._1).estimate
          val rc = cms_r_bc.value.frequency(kv._1).estimate
          val leftReplication = if(skewType.right) max(min((rc * replicationFactor).toInt, numPartitions), 1) else 1
          val rightReplication = if(skewType.left) max(min((lc * replicationFactor).toInt, numPartitions), 1) else 1

          getReplicationFactors(random, leftReplication, rightReplication).map(rl => (kv._1, (rl.swap._1, rl.swap._2), kv._2))
        }
      }

      val rightSalted = rt.mapPartitions { it =>
        val random = new Random(1)
        it.flatMap { kv =>
          val lc = cms_l_bc.value.frequency(kv._1).estimate
          val rc = cms_r_bc.value.frequency(kv._1).estimate
          val leftReplication = if(skewType.right) max(min((rc * replicationFactor).toInt, numPartitions), 1) else 1
          val rightReplication = if(skewType.left) max(min((lc * replicationFactor).toInt, numPartitions), 1) else 1

          getReplicationFactors(random, rightReplication, leftReplication).map(lr => (kv._1, (lr._1, lr._2), kv._2))
        }
      }

      //      val joined = leftSalted.joinWith(rightSalted, leftSalted("_1") === rightSalted("_1"))
      //.map { j => (j._1._2, j._2._2) }//.toDF

      //    joined.printSchema()
      //
      //joined.timedSaveToDisk("salted joined")

      (leftSalted, rightSalted)

    }


    // Start of joinExprs DSL
    def skewJoin[W: TypeTag](right: Dataset[(K, W)], joinExprs: Column, joinType: String = "inner",
                             skewJoinConfig: SkewJoinConf = SkewJoinConf()): DataFrame = {

      implicit val ek = ExpressionEncoder[K]
      implicit val ev = ExpressionEncoder[V]
      implicit val ew = ExpressionEncoder[W]
      val (l_salted, r_salted) = saltingForSkewJoin(left, right, skewJoinConfig)

      l_salted.printSchema()
      r_salted.printSchema()

      val saltExpr = (l_salted("_2") === r_salted("_2")) // and (left("_1._3") === right("_1._3"))
      // This expression is only for salting. Keys might be different
      l_salted.join(r_salted, joinExprs.and(saltExpr), joinType)
    }


    // Start of typed DSL
    def skewJoinWith[W: TypeTag](other: Dataset[(K, W)], condition: Column, joinType: String = "inner",
                                 skewJoinConfig: SkewJoinConf = SkewJoinConf()) = {
      //: Dataset[(V, W)] = {
      implicit val ek = ExpressionEncoder[K]
      implicit val ev = ExpressionEncoder[V]
      implicit val ew = ExpressionEncoder[W]
      val (l_salted, r_salted) = saltingForSkewJoin(left, other, skewJoinConfig)

      // TODO do i have to do anything here?

      val saltCondition = (left(SALT_1) === other(SALT_1)) and (left(SALT_2) === other(SALT_2))
      l_salted.joinWith(r_salted, condition and saltCondition, joinType)
    }

  }

}

