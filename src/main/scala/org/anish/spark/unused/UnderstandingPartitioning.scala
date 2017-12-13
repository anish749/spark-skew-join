package org.anish.spark.unused

import org.anish.spark.skew.Utils
import org.apache.spark.sql.SparkSession

/**
  * Created by anish on 24/10/17.
  */
object UnderstandingPartitioning {

  case class SameHashCodeDiffEquals(value: Int) {
    override def hashCode(): Int = {
      1
    }

    override def equals(obj: scala.Any): Boolean = super.equals(obj)
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[8]").appName("SkewJoin").getOrCreate()
    import Utils.dataset
    import sparkSession.implicits._

    val list1000ones = 1 to 1000 map (i => 1)

    val ones = sparkSession.sparkContext.parallelize(list1000ones).toDS()
    println("A 1000 ones in a dataset")
    ones.showPartitionStats()

    val list4000twos = 1 to 4000 map (i => 2)

    val unioned = list1000ones union list4000twos

    val onesAndTwos = sparkSession.sparkContext.parallelize(unioned).toDS()
    println("A 1000 ones and 4000 twos in a dataset")
    onesAndTwos.showPartitionStats()


    println("A 1000 ones and 4000 twos in a dataset, repartitioned into 5 based on value")
    val repartitioned = onesAndTwos
      .repartition(5, 'value)
    repartitioned.showPartitionStats()


    val list1000SameHashCodeOne = 1 to 1000 map (i => SameHashCodeDiffEquals(1))
    val list4000SameHashCodeTwo = 1 to 4000 map (i => SameHashCodeDiffEquals(2))

    val unionedSameHashCode = list1000SameHashCodeOne union list4000SameHashCodeTwo

    val sameHashCodeDS = sparkSession.sparkContext.parallelize(unionedSameHashCode).toDS()
    println("A 1000 ones and 4000 twos in a dataset with hash collision in hashCode() function")
    sameHashCodeDS.showPartitionStats()

    println("A 1000 ones and 4000 twos in a dataset with hash collision in hashCode() function, repartitioned into 5 based on value")
    sameHashCodeDS
      .repartition(5, 'value)
      .showPartitionStats()

    println("Join Normal and Repartitioned ")
    onesAndTwos.join(repartitioned, "value")
      .showPartitionStats()


    val listOneTo100 = 1 to 1000
    val listOneTo100And4000Twos = listOneTo100 union list4000twos
    val listOneTo100And4000TwosDS = sparkSession.sparkContext.parallelize(listOneTo100And4000Twos).toDS()

    println("listOneTo100And4000TwosDS")
    listOneTo100And4000TwosDS.showPartitionStats()

    println("listOneTo100And4000TwosDS Repartitioned into 5 based on value")
    listOneTo100And4000TwosDS.repartition(5, 'value)
      .showPartitionStats()

  }
}
