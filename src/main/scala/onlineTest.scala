import org.apache.spark.sql.{SparkSession, types}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object onlineTest {

    def main(args: Array[String]): Unit = {

       Logger.getLogger("org").setLevel(Level.ERROR)
       val datadir = "C:/Oracle/Big_Data/SparkScalaCourse/SparkScalaCourse/data/data/"
       val spark = SparkSession.builder().appName("sparkDemo").master("local[*]")
         .getOrCreate()

        val movies_schema = new StructType()
          .add("MovieID",IntegerType,true)
          .add("Title",StringType,true)
          .add("Genres",StringType,true)

        val ratings_schema = new StructType()
          .add("UserID",IntegerType,true)
          .add("MovieID",IntegerType,true)
          .add("Rating",IntegerType,true)
          .add("Timestamp",StringType,true)

        val users_schema = new StructType()
          .add("UserID",IntegerType,true)
          .add("Gender",StringType,true)
          .add("Age",IntegerType,true)
          .add("Occupation",IntegerType,true)
          .add("Zipcode",StringType,true)

        def parseLine(line:String) = {
          val fields = line.split("::")
          val movieid = fields(1).toInt
          val ratings = fields(2).toInt
          (movieid,ratings)
        }

        val rdd_ratings = spark.sparkContext.textFile(datadir + "ratings.txt")
        // Eliminate columns . i.e. get only user movieID and rating
        val rating_1 = rdd_ratings.map(parseLine)
        //for (x <-  rating_1.take(5)) println(x)
        //rating_1.count()
        import scala.math.min
        //Assignn value 1 and add
        val result =   rating_1.mapValues(x => (x,1) )
//        for(x <- result.take(5)) println(x)
          val  result_1 = result.reduceByKey( (x,y) => (x._1 + y._1 , x._2 + y._2))
//          for(x <- result1.take(5)) println(x)
        val ratingsavg = result_1.mapValues(x => (x._2,(x._1.toFloat/x._2.toFloat).toFloat)) //.map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1))
        //val ratingsavg01 = result1.mapValues(x => (x._1.toFloat/x._2.toFloat).toFloat).map(x => (x._2, x._1)).sortByKey(false).
        //ratingsavg.count()
        // for(x <- ratingsavg.take(5)) println(x)
        //ratingsavg.
        println("..Starts Question 1 by rdd..........")
        println("(MovieId,(No.ofuesrs,Average Rating))")
        for(x<- ratingsavg.take(10)) println(x)
      println("..Ends Question 1 by rdd..........")

    def parseLine1(line: String) = {
      val fields = line.split("::")
      val genres = fields(2)
      (genres)
    }
    val rdd_movies = spark.sparkContext.textFile(datadir + "movies.txt")
    val rdd_users = spark.sparkContext.textFile(datadir + "users.txt")
   // println("testings1", rdd_movies.count())
    val movies1 = rdd_movies.map(parseLine1)
    val movies = movies1.flatMap(x => x.split("\\|"))
    val totalBymovies = movies.map(x => (x,1) ).reduceByKey( (x,y) => (x.toInt + y.toInt)).map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1))
    //val totalmovies = totalBymovies.mapValues(x => x._1/x._2)
    //val totalBymovies1 = movies.map(x => (x,1) ).reduceByKey( (x,y) => (x.toInt + y.toInt)).map((x,(y,z)) => (x,z))
    //println("testings2",rdd_users.count())
    //for(x <- totalBymovies1.take(5)) println(x)
    //val ratings = rdd_ratings.map(x => x.split("::"))
    //println("ratingsavg1")
//    for(x <- Range(1,colnamesmovies(i).take(5)) println(x)(i)
    //for(x <- movies.take(5)) println(x)
      println("..Starts Question 2 by rdd............")
      println("(Genre,No.of Movies)")
    for(x <- totalBymovies.take(20)) println(x)
      println(".......Ends Question 2 by RDD........")

      println("\n..Question 1 by Dataframe..........")
      val df_movies = spark.read.schema(movies_schema).option("InferSchema", "false").option("sep", "::").csv(datadir + "movies.txt" )
      val df_ratings = spark.read.schema(ratings_schema).option("InferSchema", "false").option("sep", "::").csv(datadir + "ratings.txt" )
      val df_users = spark.read.schema(users_schema).option("InferSchema", "false").option("sep", "::").csv(datadir + "users.txt" )
      val csv1 = df_ratings.groupBy(col("MovieID"))
        .agg(
          count("*").as("No_of_Users"),
          avg("Rating").as("Avg_Rating")
        )
        .orderBy(desc("Avg_Rating"))
      println("..Ends Question 1 by DataFrame..........")
       csv1.show(10,false)
      //df_movies.select(col("*"),explode_outer(split(col("Genres"), "\\|"))).show(false)
      val df02 = df_movies.select(col("*"),explode_outer(split(col("Genres"), "\\|")).as("Genrecat"))
     // df02.show(5)
      val df03 = df02.groupBy("GenreCat").count().alias("counting").orderBy((desc("counting.count"))) //.show()
      //.groupBy(col("Test1")).pivot(col("Test1")).agg(countDistinct("Test1")).show()
      println("\n..Starts Question 2 by DataFrame..........")
      df03.show(20,false)
      println("..Ends Question 2 by DataFrame..........")
      val df_input = spark.read.option("header","true").option("InferSchema", "true").option("sep", "|").csv(datadir + "input.csv")
      val df01 = df_ratings.groupBy("movieID").agg(count(df_ratings("Rating")).as("N_views"),
        avg("Rating").as("Average")).where("N_views > 100 ").orderBy(desc("Average"))
      val  movie_names_df = df01.join(df_movies,"movieID")  .orderBy(desc("Average"))
      println("\n..Starts Question 3 by DataFrame..........")
      df01.orderBy(desc("Average")).withColumn("seq",lit(0))
        .withColumn("Rank", row_number() over(Window.partitionBy("seq").orderBy("seq"))).drop("seq").show(100,false)
      movie_names_df.withColumn("seq",lit(0))
        .withColumn("Rank", row_number() over(Window.partitionBy("seq").orderBy("seq"))).drop("seq","Genres").show(100,false)
      println("..Ends Question 3 by DataFrame..........")

    }

}
