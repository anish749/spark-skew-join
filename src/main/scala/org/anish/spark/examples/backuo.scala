package org.anish.spark.examples

/**
  * Created by anish on 01/11/17.
  */
class backuo {

}
/*def skewJoin(right: DataFrame, usingColumns: Seq[String], joinType: String,
              skewJoinConfig: SkewJoinConfig = SkewJoinConfig()): DataFrame = {
   import left.sparkSession.implicits._
   import skewJoinConfig._
   import Utils.datasetcms
   import CMSHasherImplicits._


   val lt_encoder: Encoder[(String, Row)] = ExpressionEncoder.tuple(ExpressionEncoder[String], RowEncoder(left.schema))
   val rt_encoder: Encoder[(String, Row)] = ExpressionEncoder.tuple(ExpressionEncoder[String], RowEncoder(right.schema))

   val lt_cms = left.map(row => usingColumns.map(row.getAs[Any]).mkString("_")) //(lt_encoder) // Custom keys should define CMS Hasher
     .getCMS(CMSeps, CMSdelta, CMSseed)
   //.getCmsForKey(CMSeps, CMSdelta, CMSseed)

   val rt_cms = right.map(row => usingColumns.map(row.getAs[Any]).mkString("_"))
     .getCMS(CMSeps, CMSdelta, CMSseed)

   //(usingColumns.reduce((x, y) => row.getAs[String](x) + "_" + row.getAs[String](y)), row) }
   // Does the join Stringly. Will this break for complex / struct types?
   val rt = right.map { row => (usingColumns.map(row.getAs[Any]).mkString("_"), row) }(rt_encoder)
   //.getCmsForKey(CMSeps, CMSdelta, CMSseed)


   val salted_left_encoder: Encoder[(Int, Int, Row)] = ExpressionEncoder.tuple(ExpressionEncoder[Int],ExpressionEncoder[Int], RowEncoder(left.schema))
   val salted_left = left.mapPartitions { it =>
     it.flatMap { kv => (0 until 5) map (x => (1, 2, kv))
     }
   } (salted_left_encoder)

   salted_left.printSchema()


   val salted_rt_encoder: Encoder[(Int, Int, Row)] = ExpressionEncoder.tuple(ExpressionEncoder[Int],ExpressionEncoder[Int], RowEncoder(right.schema))
   val salted_rt = left.mapPartitions { it =>
     it.flatMap { kv => (0 until 5) map (x => (1, 2, kv))
     }
   } (salted_rt_encoder)

   salted_rt.printSchema()

   salted_left.join(salted_rt).printSchema()

   //val (l_salted, r_salted) = FrequencyBasedSalting.salt(lt, rt, skewJoinConfig)(lt_encoder, rt_encoder)
   //
   //      val l_saltAdded = l_salted.map { x =>
   //        val s1 = x._1._2._1
   //        val s2 = x._1._2._2
   //        Row.fromSeq(x._2.toSeq :+ s1 :+ s2)
   //      }
   //
   //      val r_saltAdded = r_salted.map { x =>
   //        val s1 = x._1._2._1
   //        val s2 = x._1._2._2
   //        Row.fromSeq(x._2.toSeq :+ s1 :+ s2)
   //      } // TODO Where do you add the scheme?

   // TODO generate KV pair Dataset yourself for all using cols => We will assume it is a string

   // TODO now how do we map this?
   // Answer - Add the two cols to both the data frames and then send to the lower call
   //l_saltAdded.join(r_saltAdded, usingColumns ++ SALTS, joinType) // TODO where did you add the salting cols

   left.join(right)
 }*/