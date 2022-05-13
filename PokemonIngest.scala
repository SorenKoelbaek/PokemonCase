// Databricks notebook source
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


//We define the schema of our iterator requests; adding the object array to our schema object.
val PokemonIteraterObject = (new StructType)
        .add("name", StringType)
        .add("url", StringType)

val pokemonIteraterResults = new ArrayType(PokemonIteraterObject, false)

val PokemonIteraterSchema: StructType = (new StructType)
  .add("results", pokemonIteraterResults)
  .add("next", StringType)



//We are doing a lazy while loop to iterate over the default API Pagination
//checking the next value and extracting the default (20) pokemons at each request.
  // --> this would benefit of a recursive function instead; change if time allows.
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

var responseList = new scala.collection.mutable.ListBuffer[String]()

for (PokeUrl <- pokemonURLlist )
{
     val httpRequest = new HttpRequest;
    responseList += httpRequest.ExecuteHttpGet(PokeUrl)                              
}

val pokemons_ds = responseList.toDS()
val Pokemons_df = spark.read.json(pokemons_ds)


  Pokemons_df
    .withColumn("current_timestamp",current_timestamp())
    .withColumn("date",to_date(col("current_timestamp")))
    .write.mode("overwrite")
    .partitionBy("date")
    .format("json")
    .save(LandinDir);
  



// COMMAND ----------


//val df = spark.read.json("/FileStore/raw/Pokemon/")

//dbutils.fs.rm("/FileStore/raw/Pokemon/",true)


// COMMAND ----------


