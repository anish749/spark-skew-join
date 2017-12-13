package org.anish.spark.unused

import org.apache.spark.Partitioner

/**
  * Partition the data based on the following:
  * If id is less than a threshold, keep data as is
  * Else, divide those partitions into smaller blocks based on salt.
  *
  * Input is a PairRDD with key as (id, salt) where id is the partition index and salt is a random number between 0 and salting factor
  *
  * Created by anish on 23/10/17.
  */
class SaltedIsolatedPartitioner(initialPartitions: Int,
                                repartitionIndexStart: Int,
                                saltingFactor: Int) extends Partitioner {

  require(repartitionIndexStart <= initialPartitions, "repartition threshold is more than or same as initial partitions")
  require(saltingFactor >= 1, "repartition into should be greater than 0")

  override def numPartitions: Int =
    repartitionIndexStart + (initialPartitions - repartitionIndexStart) * saltingFactor

  override def getPartition(key: Any): Int = {
    key match {
      case (id: Long, salt: Int) => // This is the key // id is original id, salt should be 0..salting factor
        if (id >= repartitionIndexStart)
          repartitionIndexStart + (id - repartitionIndexStart) * saltingFactor + salt toInt // We must return an int since there can be only integer number of partitions.
        else
          id.toInt // id was originally the partition index
      case x => throw new IllegalArgumentException(s"SaltedIsolatedPartitioner is not designed for $x")
    }
  }
}
