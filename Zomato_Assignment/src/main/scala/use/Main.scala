package use

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object main {
  def main(args:Array[String]): Unit= {
    val spark = SparkSession.builder()
      .appName("zomatoDataAnalysis")
      .master("local[*]")
      .getOrCreate()

    val headerSchema = StructType(Array(
      StructField("ID", IntegerType, nullable = true),
      StructField("Name", StringType, nullable = true),
      StructField("Rating", StringType, nullable = true),
      StructField("Votes", IntegerType, nullable = true),
      StructField("Location", StringType, nullable = true),
      StructField("RestaurantType", StringType, nullable = true),
      StructField("Highlights", StringType, nullable = true),
      StructField("Cuisines", StringType, nullable = true),
      StructField("Cost", IntegerType, nullable = true)
    ))

    val zomatoData: DataFrame = spark.read
      .option("header", "false")
      .schema(headerSchema)
      .csv("src/Resources/zomato_cleaned-zomato_cleaned.csv")

    spark.stop()
  }
  //zomatoData.show(10)
  def topNRestaurantsByRating(df:DataFrame,n:Int):DataFrame ={
    df.withColumn("Rating",regexp_replace(col("Rating"),"/5","").cast(DoubleType))
      .orderBy(desc("Rating"))
      .limit(n)
  }

  def getTopNRestaurantsByRatingAndLocation(df: DataFrame, n: Int, location: String, restaurantType: String): DataFrame = {
    df.withColumn("Rating", regexp_replace(col("Rating"), "/5", "").cast(DoubleType))
      .filter(col("Location") === location && col("RestaurantType") === restaurantType)
      .orderBy(desc("Rating"))
      .limit(n)
  }

  def getTopNRestaurantsByRatingAndVotes(df:DataFrame,n:Int,location:String):DataFrame={
    df.withColumn("Rating",regexp_replace(col("Rating"),"/5","").cast(DoubleType))
      .filter(col("Location")===location)
      .orderBy(desc("Rating"),desc("Votes"))
      .limit(n)
  }

  def getDishesLikedCount(df:DataFrame):DataFrame={
    df.withColumn("DishesLikedCount",size(split(col("Highlights"),","))).select("Name","DishesLikedCount")
  }

  def getDishesLocations(df:DataFrame):Long={
    df.select("Location").distinct().count()
  }

  def getDistinctCuisinesInLocation(df: DataFrame, location: String): DataFrame = {
    df.filter(col("Location") === location)
      .withColumn("Cuisine", explode(split(trim(col("Cuisines")), ",")))
      .withColumn("Cuisine", trim(col("Cuisine"))) // Trim whitespace around cuisines
      .select("Cuisine")
      .distinct()
  }


  def getDistinctCuisinesInEachLocation(df:DataFrame):DataFrame= {
    df.withColumn("Cuisine",explode(split(col("Cuisines"),",")))
      .groupBy("Location")
      .agg(countDistinct("Cuisine").as("DistinctCuisines"))
      .select("Location","DistinctCuisines")
  }

  def getCuisineTypeCount(df: DataFrame): DataFrame = {
    df.withColumn("Cuisine", explode(split(col("Cuisines"), ",")))
      .withColumn("Cuisine", trim(col("Cuisine")))  // Trim spaces around cuisine names
      .groupBy("Cuisine")
      .count() // Rename column to match expected output
  }
}