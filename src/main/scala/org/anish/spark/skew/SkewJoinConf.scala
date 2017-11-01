package org.anish.spark.skew

/**
  * Created by anish on 01/11/17.
  */
case class SkewJoinConf(CMSeps: Double = 0.005, CMSdelta: Double = 1e-8, CMSseed: Int = 1,
                        replicationFactor: Double = 1e-3,
                        skewType: SkewType = CrossSkew)