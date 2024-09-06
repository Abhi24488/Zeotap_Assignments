package use
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class MainTest extends AnyFunSuite {

  val spark: SparkSession = SparkSession.builder()
    .appName("zomatoDataAnalysisTests")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // Sample data for testing
  val zomatoData: DataFrame = Seq(
    (1, "Restaurant A", "4.5/5", 200, "Indiranagar", "Cafe", "Good service, Cozy atmosphere", "Indian, Chinese", 1000),
    (2, "Restaurant B", "4.8/5", 150, "Indiranagar", "Cafe", "Delicious food", "Italian, Continental", 1500),
    (3, "Restaurant C", "3.0/5", 50, "Koramangala", "Diner", "Fast service", "Mexican, Indian", 500),
    (4, "Restaurant D", "4.0/5", 100, "Indiranagar", "Cafe", "Outdoor seating", "Italian, Chinese", 1200),
    (5, "Restaurant E", "3.5/5", 80, "Koramangala", "Diner", "Good ambiance", "Thai, Indian", 800)
  ).toDF("ID", "Name", "Rating", "Votes", "Location", "RestaurantType", "Highlights", "Cuisines", "Cost")

  // Tests

  test("Top N Restaurants by Rating") {
    val topRestaurants = main.topNRestaurantsByRating(zomatoData, 2)
    assert(topRestaurants.count() == 2, "Test failed: should return top 2 restaurants by rating")
    assert(topRestaurants.filter($"Rating" > 5.0 || $"Rating" < 0.0).count() == 0, "Test failed: Ratings should be between 0 and 5")
  }

  test("Top N Restaurants by Rating and Location") {
    val topRestaurantsByLocation = main.getTopNRestaurantsByRatingAndLocation(zomatoData, 2, "Indiranagar", "Cafe")
    assert(topRestaurantsByLocation.count() == 2, "Test failed: should return top 2 cafes in Indiranagar")
    assert(topRestaurantsByLocation.filter($"Location" =!= "Indiranagar").count() == 0, "Test failed: Restaurants should be from Indiranagar")
    assert(topRestaurantsByLocation.filter($"RestaurantType" =!= "Cafe").count() == 0, "Test failed: Restaurants should be Cafes")
  }

  test("Top N Restaurants by Rating and Votes") {
    val topRestaurantsByRatingAndVotes = main.getTopNRestaurantsByRatingAndVotes(zomatoData, 1, "Indiranagar")
    assert(topRestaurantsByRatingAndVotes.count() == 1, "Test failed: should return 1 restaurant by rating and votes in Indiranagar")
    assert(topRestaurantsByRatingAndVotes.filter($"Location" =!= "Indiranagar").count() == 0, "Test failed: Restaurants should be from Indiranagar")
  }

  test("Dishes Liked Count") {
    val dishesLikedCount = main.getDishesLikedCount(zomatoData)
    assert(dishesLikedCount.filter($"DishesLikedCount".isNull).count() == 0, "Test failed: No null values should be in DishesLikedCount")
  }

  test("Distinct Locations Count") {
    val distinctLocations = main.getDishesLocations(zomatoData)
    assert(distinctLocations == 2, "Test failed: There should be 2 distinct locations")
  }

  test("Distinct Cuisines in Location") {
    val distinctCuisinesInLocation = main.getDistinctCuisinesInLocation(zomatoData, "Indiranagar")
    assert(distinctCuisinesInLocation.count() == 4, "Test failed: There should be 3 distinct cuisines in Indiranagar")
  }

  test("Distinct Cuisines in Each Location") {
    val distinctCuisinesInEachLocation = main.getDistinctCuisinesInEachLocation(zomatoData)
    assert(distinctCuisinesInEachLocation.filter($"DistinctCuisines".isNull).count() == 0, "Test failed: DistinctCuisines should not contain null values")
    assert(distinctCuisinesInEachLocation.count() == 2, "Test failed: There should be 2 distinct location entries")
  }

  test("Cuisine Type Count") {
    val cuisineTypeCount = main.getCuisineTypeCount(zomatoData)
    assert(cuisineTypeCount.filter($"count".isNull).count() == 0, "Test failed: Count column should not contain null values")
    assert(cuisineTypeCount.count() == 6, "Test failed: There should be 6 different cuisine types")
  }
}

