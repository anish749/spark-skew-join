/** (C) Koninklijke Philips Electronics N.V. 2017
*
* All rights are reserved. Reproduction or transmission in whole or in part, in any form or by any
* means, electronic, mechanical or otherwise, is prohibited without the prior written consent of
* the copyright owner.
*
* File name : SparkContextFactory.scala
*/
package org.anish.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * A lazy evaluated Spark Session for managing Spark Context and SQL Context for Spark 1.6 or less.
  *
  * Created by anish on 25/04/17.
  */
object SimpleSparkSession {

  // Lazy val is used to cache
  lazy val sparkContext: SparkContext = {
    val sparkConf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("Spark-Unit-Tests")
      .set("spark.driver.host", "localhost")
    new SparkContext(sparkConf)
  }

  lazy val sqlContext: SQLContext = {
    new SQLContext(sparkContext)
  }
}
