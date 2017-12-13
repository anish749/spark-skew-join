package org.anish.spark.unused

import vegas._

/**
  * Created by anish on 19/10/17.
  */
object UnderstandingHistograms {
  def main(args: Array[String]): Unit = {

    val plot = Vegas("Country Pop").
      withData(
        Seq(
          Map("country" -> "USA", "population" -> 314),
          Map("country" -> "UK", "population" -> 64),
          Map("country" -> "DK", "population" -> 80)
        )
      ).
      encodeX("country", Nom).
      encodeY("population", Quant).
      mark(Bar)

    plot.show



  }
}
