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


    val topRestaurantsByRating=topNRestaurantsByRating(zomatoData, 5)
    assert(topRestaurantsByRating.filter(col("Rating")<0.0 || col("Rating")>5.0).count()==0,"Test Failed found invalid ratings")
    println("Tests Passed: Top N Restaurants By Rating")

        val topRestaurantsByLocationAndType=getTopNRestaurantsByRatingAndLocation(zomatoData,5,"Indiranagar","Cafe")
        assert(topRestaurantsByLocationAndType.filter(col("Location") =!= "Indiranagar").count()==0,"Test failed restaurants should be from Indiranagar")
        assert(topRestaurantsByLocationAndType.filter(col("RestaurantType") =!= "Cafe").count() == 0, "Test failed: Restaurants should be Cafes")
        println("Tests Passed: Top N restaurants by location and type")

        val topRestaurantsByRatingAndVotes=getTopNRestaurantsByRatingAndVotes(zomatoData,5,"Indiranagar")
        assert(topRestaurantsByRatingAndVotes.filter(col("Location") =!= "Indiranagar").count() == 0, "Test failed: Restaurants should be from Indiranagar")
        println("Tests Passed: Top N Restaurants in a location by Rating And Votes")

        val dishesLikedCount = getDishesLikedCount(zomatoData)
        assert(dishesLikedCount.columns.contains("DishesLikedCount"), "Test failed: should have a DishesLikedCount column")
        assert(dishesLikedCount.filter(col("DishesLikedCount").isNull).count() == 0, "Test failed: DishesLikedCount should not contain null values")
        println("Tests passed: No. of dishes liked in every restaurant")

        val distinctLocations = getDishesLocations(zomatoData)
        assert(distinctLocations > 0, "Test failed: getDishesLocations should return a positive number of distinct locations")
        println("Tests Passed: No. of distinct locations")

        val distinctCuisinesInLocation = getDistinctCuisinesInLocation(zomatoData, "Indiranagar")
        assert(distinctCuisinesInLocation.count() > 0, "Test failed: getDistinctCuisinesInLocation should return distinct cuisines")
        println("Tests Passed: No. of distinct cuisines for the specified location")

        val distinctCuisinesInEachLocation = getDistinctCuisinesInEachLocation(zomatoData)
        assert(distinctCuisinesInEachLocation.columns.contains("DistinctCuisines"), "Test failed: getDistinctCuisinesInEachLocation should have a DistinctCuisines column")
        assert(distinctCuisinesInEachLocation.filter(col("DistinctCuisines").isNull).count() == 0, "Test failed: DistinctCuisines column should not contain null values")
        println("Tests Passed: No. of distinct cuisines at each location")

        val cuisineTypeRestaurantCount = getCuisineTypeCount(zomatoData)
        //cuisineTypeRestaurantCount.show()
        assert(cuisineTypeRestaurantCount.columns.contains("count"), "Test failed: getCuisineTypeCount should have a count column")
        assert(cuisineTypeRestaurantCount.filter(col("count").isNull).count() == 0, "Test failed: Count column should not contain null values")
        println("Tests Passed: Count of restaurants for each cuisine type")

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