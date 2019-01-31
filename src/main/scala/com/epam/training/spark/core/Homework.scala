package com.epam.training.spark.core

import com.epam.training.spark.core.domain.Climate
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Homework {
  val DELIMITER = ";"
  val RAW_BUDAPEST_DATA = "data/budapest_daily_1901-2010.csv"
  val OUTPUT_DUR = "output"

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("EPAM BigData training Spark Core homework")
      .setIfMissing("spark.master", "local[2]")
      .setIfMissing("spark.sql.shuffle.partitions", "10")
    val sc = new SparkContext(sparkConf)

    processData(sc)

    sc.stop()

  }

  def processData(sc: SparkContext): Unit = {

    /**
      * Task 1
      * Read raw data from provided file, remove header, split rows by delimiter
      */
    val rawData: RDD[List[String]] = getRawDataWithoutHeader(sc, Homework.RAW_BUDAPEST_DATA)

    /**
      * Task 2
      * Find errors or missing values in the data
      */
    val errors: List[Int] = findErrors(rawData)
    println(errors)

    /**
      * Task 3
      * Map raw data to Climate type
      */
    val climateRdd: RDD[Climate] = mapToClimate(rawData)

    /**
      * Task 4
      * List average temperature for a given day in every year
      */
    val averageTemeperatureRdd: RDD[Double] = averageTemperature(climateRdd, 1, 2)

    /**
      * Task 5
      * Predict temperature based on mean temperature for every year including 1 day before and after
      * For the given month 1 and day 2 (2nd January) include days 1st January and 3rd January in the calculation
      */
    val predictedTemperature: Double = predictTemperature(climateRdd, 1, 2)
    println(s"Predicted temperature: $predictedTemperature")

  }

  def getRawDataWithoutHeader(sc: SparkContext, rawDataPath: String): RDD[List[String]] = {
    val rddRaw = sc.textFile(rawDataPath)
    rddRaw.filter(!_.startsWith("#")).map(_.split(";", 7).toList)
  }

  def findErrors(rawData: RDD[List[String]]): List[Int] = {
    val rddErrors = rawData.map(sl => sl.map(x => if (x == "") 1 else 0))
    rddErrors.reduce((ls1, ls2) => List(
        ls1(0) + ls2(0)
      , ls1(1) + ls2(1)
      , ls1(2) + ls2(2)
      , ls1(3) + ls2(3)
      , ls1(4) + ls2(4)
      , ls1(5) + ls2(5)
      , ls1(6) + ls2(6)
    ))
  }

  def mapToClimate(rawData: RDD[List[String]]): RDD[Climate] = {
    rawData.map(x => Climate.apply(x(0), x(1), x(2), x(3), x(4), x(5), x(6)))
  }

  def averageTemperature(climateData: RDD[Climate], month: Int, dayOfMonth: Int): RDD[Double] = {
    climateData
      .filter(x => (x.observationDate.getMonthValue == month) && (x.observationDate.getDayOfMonth == dayOfMonth))
      .map(_.meanTemperature.value)
  }

  def predictTemperature(climateData: RDD[Climate], month: Int, dayOfMonth: Int): Double = {
    val rddFilteredValues = climateData
      .filter(x =>
          ((x.observationDate.getMonthValue == month) && (x.observationDate.getDayOfMonth == dayOfMonth))
        ||
          ((x.observationDate.plusDays(1).getMonthValue == month) && (x.observationDate.plusDays(1).getDayOfMonth == dayOfMonth))
        ||
          ((x.observationDate.minusDays(1).getMonthValue == month) && (x.observationDate.minusDays(1).getDayOfMonth == dayOfMonth))
        ).map(_.meanTemperature.value)
//        val rddFilteredValues = climateData.map(_.meanTemperature.value)
    rddFilteredValues.reduce(_ + _) / rddFilteredValues.count()
  }

}


