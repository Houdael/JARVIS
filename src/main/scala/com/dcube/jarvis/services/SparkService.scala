package com.dcube.jarvis.services

import org.apache.spark.sql.SparkSession

/**
 * Base class for all services using Spark
 */
abstract class SparkService {
  protected val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
}
