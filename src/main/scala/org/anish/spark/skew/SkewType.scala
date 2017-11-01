package org.anish.spark.skew

/**
  * Created by anish on 01/11/17.
  */
sealed trait SkewType {
  val left = false
  val right = false
}

/**
  * A left skew means that one value in the left side appears multiple times. This should be fragmented.
  */
case object LeftSkew extends SkewType {
  override val left = true
}

case object RightSkew extends SkewType {
  override val right = true
}

/**
  * // Both sides can have skew
  */
case object CrossSkew extends SkewType {
  override val left = true
  override val right = true
}