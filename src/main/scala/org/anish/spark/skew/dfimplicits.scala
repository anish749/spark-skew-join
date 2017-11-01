
package org.anish.spark.skew

import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import com.twitter.algebird.{CMSHasher, CMSHasherImplicits}
import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, concat, explode, udf}

import scala.math.{max, min}
import scala.reflect.runtime.universe._
import scala.util.Random

/**
  * Created by anish on 31/10/17.
  */
object dfimplicits {
  private final val JOIN_KEY = "_join_key_nbRcsSD1FW"
  private final val SALT_1 = "_salt_c3O4hPezNs_1"
  private final val SALT_2 = "_salt_bpyWIjwfIy_2"
  private final val SALTS = Seq(SALT_1, SALT_2)

  implicit class SkewJoinOnDataFrame(left: DataFrame) {

    // usingColumn DSL
    def skewJoin(right: DataFrame, usingColumn: String): DataFrame = {
      skewJoin(right, Seq(usingColumn))
    }

    // This is only meant for data frames
    def skewJoin(right: DataFrame, usingColumns: Seq[String], joinType: String = "inner",
                 skewJoinConf: SkewJoinConf = SkewJoinConf()): DataFrame = {
      val sparkSession = left.sparkSession
      import sparkSession.implicits._
      import Utils.datasetcms
      import CMSHasherImplicits._
      import skewJoinConf._

      // Config imported:
      // Parameters to use for Count Min Sketch (CMS)
      // replicationFactor - 1/rF number of records would be in 1 partition when replicated
      // SkewType - Are there skews on both sides or only one

      // Convert from sequence of columns to one column.
      // This is required to create the CMS
      val lt = left.withColumn(JOIN_KEY, concat(usingColumns.map(col): _*))
      val rt = right.withColumn(JOIN_KEY, concat(usingColumns.map(col): _*))

      // We convert to string and then use CMS[String] // Later on we can use Array[Bytes] or other serialization
      // To preserve the types, you should use JoinWith instead.
      val cms_l = lt
        .map(row => row.getAs[String](JOIN_KEY))
        .getCMS(CMSeps, CMSdelta, CMSseed)

      val cms_r = rt
        .map(row => row.getAs[String](JOIN_KEY))
        .getCMS(CMSeps, CMSdelta, CMSseed)

      val cms_l_bc = sparkSession.sparkContext.broadcast(cms_l)
      val cms_r_bc = sparkSession.sparkContext.broadcast(cms_r)

      // We will be defining UDFs which will run on each record.
      // We don't want to create multiple objects with the same seed and take only 1 value. That would remove the randomness
      val lRandom_bc = sparkSession.sparkContext.broadcast(new Random(1))
      val rRandom_bc = sparkSession.sparkContext.broadcast(new Random(2))


      val numPartitions = defaultPartitioner(left.rdd, right.rdd).numPartitions

      // Fragment Left DF
      val fragmentLeftUdf = udf { id: String => // We are using the key column as string here
        val random = lRandom_bc.value // If we set the seed here, it will always give the same value for each record.
      val lc = cms_l_bc.value.frequency(id).estimate
        //      val rc = cms_r_bc.value.frequency(id).estimate
        //      val leftReplication = max(min((rc * replicationFactor).toInt, numPartitions), 1)
        val rightReplication = if (skewType.left) max(min((lc * replicationFactor).toInt, numPartitions), 1) else 1
        // Based on left count find right replication => right should be replicated if left frequency is high

        // based on right replication, generate fragment for left
        random.nextInt(rightReplication)
      }

      val fragmentRightUdf = udf { id: String =>
        val random = rRandom_bc.value
        //      val lc = cms_l_bc.value.frequency(id).estimate
        val rc = cms_r_bc.value.frequency(id).estimate
        val leftReplication = if (skewType.right) max(min((rc * replicationFactor).toInt, numPartitions), 1) else 1
        //      val rightReplication = max(min((lc * replicationFactor).toInt, numPartitions), 1)
        // Based on right count find left replication => left should be replicated if right frequency is high

        // based on left replication, generate fragment for right
        random.nextInt(leftReplication)
      }


      // replication using explode. This will produce an array of indexes, use this to while exploding
      val replicateIndexesLeftUdf = udf { id: String => // Generate an array of replica indexes
        val rc = cms_r_bc.value.frequency(id).estimate
        val leftReplication = if (skewType.right) max(min((rc * replicationFactor).toInt, numPartitions), 1) else 1
        0 until leftReplication toArray
      }

      val replicateIndexesRightUdf = udf { id: String => // Generate an array of replica indexes
        val lc = cms_l_bc.value.frequency(id).estimate
        val rightReplication = if (skewType.left) max(min((lc * replicationFactor).toInt, numPartitions), 1) else 1
        0 until rightReplication toArray
      }


      val leftSalted = lt.withColumn(SALT_1, fragmentLeftUdf(col(JOIN_KEY))) // _1 should be K
        .withColumn(SALT_2, explode(replicateIndexesLeftUdf(col(JOIN_KEY))))

      val rightSalted = rt.withColumn(SALT_2, fragmentRightUdf(col(JOIN_KEY)))
        .withColumn(SALT_1, explode(replicateIndexesRightUdf(col(JOIN_KEY))))

      val joined_tmp = leftSalted.join(rightSalted, Array(JOIN_KEY, SALT_1, SALT_2), joinType)
        .drop(leftSalted(JOIN_KEY))
        .drop(rightSalted(JOIN_KEY))
        .drop(leftSalted(SALT_1))
        .drop(rightSalted(SALT_1))
        .drop(leftSalted(SALT_2))
        .drop(rightSalted(SALT_2))

      usingColumns.map(rightSalted.apply).foldLeft(joined_tmp)((acc, c) => acc.drop(c)) // drop the columns from the right side that were used to join
    }
  }

}

