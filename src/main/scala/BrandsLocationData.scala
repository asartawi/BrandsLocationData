package main

import java.sql.SQLException
import java.util.Properties
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col

object BrandsLocationData {
  def main (args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Users table data")
      .config("spark.driver.memory", "10g")
      .enableHiveSupport()
      .getOrCreate()

    // Set the credentials of the connection to the RDS
    val connectionProperties = new Properties()
    connectionProperties.put("user", "hanan.atallah")
    connectionProperties.put("password", "C8#+zsCTP97zyK3V")

    /*********************************************************************
      * Use spark jdbc to connect and read from the DB
      * collect data from locations, cities, countries, and states tables
       ********************************************************************/
    val brands_df = spark.read.jdbc(
      "jdbc:mysql://dev-rds-1-cluster.cluster-cvuu9acg76dy.us-east-1.rds.amazonaws.com:3306/harri3_live_20170109",
      "location",
      connectionProperties
    )

    val cities_df = spark.read.jdbc(
      "jdbc:mysql://dev-rds-1-cluster.cluster-cvuu9acg76dy.us-east-1.rds.amazonaws.com:3306/harri3_live_20170109",
      "city",
      connectionProperties
    )

    val states_df = spark.read.jdbc(
      "jdbc:mysql://dev-rds-1-cluster.cluster-cvuu9acg76dy.us-east-1.rds.amazonaws.com:3306/harri3_live_20170109",
      "state",
      connectionProperties
    )

    val countries_df = spark.read.jdbc(
      "jdbc:mysql://dev-rds-1-cluster.cluster-cvuu9acg76dy.us-east-1.rds.amazonaws.com:3306/harri3_live_20170109",
      "country",
      connectionProperties
    )

    // Join brands location data with countries, cities, states data
    val brandsLocation_df = brands_df
      .select(col("brand_id"), col("country_id"), col("city_id"),
                col("state_id"), col("latitude").as("brand_lat"),
                  col("longitude").as("brand_log"))
      .join(countries_df.select(col("id"), col("code").as("brand_country")),
        col("country_id") === col("id"),
        "left_outer"
      )
      .drop("country_id", "id")
      .join(states_df.select(col("id"), col("name").alias("brand_state")),
        col("state_id") === col("id"),
        "left_outer"
      )
      .drop("id", "state_id")
      .join(cities_df.select(col("id"), col("code").as("brand_city")),
        col("city_id") === col("id"),
        "left_outer"
      )
      .drop("city_id", "id")

    try{
      val now = System.nanoTime()

      // Combine partitions and save as a hive table in the data warehouse location of hive
      brandsLocation_df
        .coalesce(1)
        .write.mode(SaveMode.Overwrite)
        .option("header", "true")
        .saveAsTable("brandsLocations")

      val till = System.nanoTime() - now
      val now1 = System.nanoTime()
    }
    catch{
      case e: SQLException => println("Connection Error")
      case e: Exception => e.printStackTrace()
    }
  }
}