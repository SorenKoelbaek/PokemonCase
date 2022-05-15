// Databricks notebook source
// MAGIC %md # Ingestion Notebook
// MAGIC ### This notebook consists of two steps:
// MAGIC 1. First we build a list of pokemon URLS by asking the REST api endpoint **_Pokemon_**, we check the _next_ response for the next URL and continue until there is no next page. We build a listobject of individual Pokemon object URLS:
// MAGIC 2. Then we loop through the URLS, and append the response string to an object, which we lastly save into our storage Landingzone as a json file partitioned by _Current_Timestamp()_ -- as we rely on the DBFS filesystem to partition the json output as it wants

// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf, from_json, explode, sha2}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import okhttp3.{Headers, OkHttpClient, Request, Response}


val appName: String = "PokemonCase"
val spark = SparkSession
  .builder()
  .appName(appName)
  .master("local[*]")
  .getOrCreate()


//We are doing a lazy while loop to iterate over the default API Pagination
//checking the next value and extracting the default (20) pokemons at each request.
var uri = "https://pokeapi.co/api/v2/pokemon";
var hasnext = true;

//We create a list object of pokemonURLS to iterate through later
var pokemonURLlist: List[String] = List()

while(hasnext)
{
  val httpRequest = new HttpRequest;
  val source_df = spark.read.json(Seq(httpRequest.ExecuteHttpGet(uri)).toDS())
  
  var nextUrl = source_df.select(col("next")).first().getString(0);
  
//Explode the Pokemon result object and select the attributed URL and add to the list
  pokemonURLlist = List.concat(pokemonURLlist, source_df.select(explode(col("Results")).alias("pokemons"))
      .select("pokemons.url").map(f=>f.getString(0)).collect.toList)
 
  
  //Check if the next URL is set, if not stop the while loop
  if(nextUrl != null)
  {
    uri = nextUrl;
  } 
  else
  {
    hasnext = false;
  }

}



case class RestAPIRequest (url: String)

class HttpRequest {
   def ExecuteHttpGet(url: String) = {
    
    val client: OkHttpClient = new OkHttpClient();

    val headerBuilder = new Headers.Builder
    val headers = headerBuilder
      .add("content-type", "application/json")
      .build

    val result = try {
        val request = new Request.Builder()
          .url(url)
          .headers(headers)
          .build();

        val response: Response = client.newCall(request).execute()
        response.body().string()
      }
      catch {
        case _: Throwable => null
      }

    
   result
  }

 
}

// COMMAND ----------



//Now we loop through all pokemons in our Iterator-list to extract information on each pokemon and save that into our Raw filesystem alongside a watermark.

import org.apache.spark.sql.functions.{col, udf, from_json, explode, sha2, current_timestamp, to_date, lit}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.Instant

val LandinDir = "/FileStore/raw/Pokemon/"

//We use a listbuffer as it is immutable and we can add our JSON response to the list for each URL.
var responseList = new scala.collection.mutable.ListBuffer[String]()

for (PokeUrl <- pokemonURLlist )
{
     val httpRequest = new HttpRequest;
    responseList += httpRequest.ExecuteHttpGet(PokeUrl)                              
}

//We collect the dataframe from the list of JSON responses
val pokemons_ds = responseList.toDS()
val Pokemons_df = spark.read.json(pokemons_ds)

//Save out the Dataframe to our DBFS
  Pokemons_df
    //.withColumn("height",lit(100)) //Test to demonstrate SCD2 downstream
    .withColumn("current_timestamp",current_timestamp())
    .write.mode("append")
    .partitionBy("current_timestamp")
    .format("json")
    .save(LandinDir);
  



// COMMAND ----------


