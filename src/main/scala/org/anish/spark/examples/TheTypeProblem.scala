//package org.anish.spark.examples
//
//import org.apache.spark.sql._
//import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
//
//import scala.reflect.ClassTag
//
///**
//  * Created by anish on 31/10/17.
//  */
//object TheTypeProblem {
//  val sparkSession = SparkSession.builder().master("local[8]").appName("SkewJoin").getOrCreate()
//
//
//  def main(args: Array[String]): Unit = {
//
//    import sparkSession.implicits._
//    val someDF: Dataset[Row] = sparkSession
//      .read
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .load("data/skewed_stock/prices_withFeatures")
//
//
//
//
//    someDF.map { x => x.getAs[String]("symbol")
//    }
//
//
//    callF(someDF)
//  }
//
//   import scala.reflect.runtime.universe.TypeTag
//  def callF[K <: Product: TypeTag](someDF: Dataset[K]) //(implicit encoder: Encoder[K])
//  = {
//    implicit val tuple2Encoder: Encoder[(K)] =
//      ExpressionEncoder.tuple(someDF.exprEnc, someDF.exprEnc)
//    Encoders.product[K]
//    someDF.map { x => x
//    }
//  }
//
//
//}
