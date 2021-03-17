package main.java.spark.structuredstreaming.jdbccontinuous

object ScalaUtils {

  def convertMap(mutable: scala.collection.mutable.Map[String, String]) = {
    mutable.toMap
  }

}
