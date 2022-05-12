// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF1
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

//Declare a dataframe to hold our collected object --> maybe use a list instead?
var PokemonIterater_df = spark.createDataFrame(spark.sparkContext
      .emptyRDD[Row], PokemonIteraterObject)

while(hasnext)
{
  val httpRequest = new HttpRequest;
  val source_df = spark.read.json(Seq(httpRequest.ExecuteHttpGet(uri)).toDS())
  
  var nextUrl = source_df.select(col("next")).first().getString(0);
  
//Explode the Pokemon result object alongside the attributed URL
  val delta_df =  source_df.select(explode(col("Results")).alias("pokemons"))
      .select(col("pokemons.name"), col("pokemons.url"))
 
  PokemonIterater_df = PokemonIterater_df.union(delta_df);
  
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

PokemonIterater_df.cache()

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


//Now we loop through all pokemons in our Iterator-list to extract information on each pokemon and save that into our Raw filesystem alongside a watermark and extraction URL.
import org.apache.spark.sql.functions.current_timestamp   

case class pokemonUrl(name:String, url:String)

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.Instant

//We store our Extracted data, the only manipulation we've done so far is to identify and split our PII data into two different stores, in order for our Pseudonymisation to work!
val now: Timestamp = Timestamp.from(Instant.now())
val date = new SimpleDateFormat("yyyyMMdd").format(now)

val DirPseudo = "/FileStore/raw/Pokemon_Pseudonymised/".concat(date).concat("/")
val dirIdentifier = "/FileStore/raw/Pokemon_Identifier/".concat(date).concat("/")


PokemonIterater_df.as[pokemonUrl].take(PokemonIterater_df.count.toInt).foreach(t => 
{
 val uri = t.url
 val name = t.name

 val httpRequest = new HttpRequest;
 val source_df = spark.read.json(Seq(httpRequest.ExecuteHttpGet(uri)).toDS())
  //We split our data into two, (identifiable or not) and use sha_256 to encrypt our unique pokemon name.
  //I drop the moves variable as it is huge.
 val identifiable_df = source_df.select(col("name"), col("id"), col("species"), col("forms")).withColumn("Identifier",sha2(col("name"),256));
 val pseudonymised_df = source_df.withColumn("Identifier",sha2(col("name"),256)).drop("name").drop("id").drop("species").drop("forms").drop("moves");

  identifiable_df.withColumn("current_timestamp",current_timestamp().as("current_timestamp")).write.mode("overwrite").format("json").save(dirIdentifier);
  pseudonymised_df.withColumn("current_timestamp",current_timestamp().as("current_timestamp")).write.mode("overwrite").format("json").save(DirPseudo);
 
})

//Rewrite again, to increase performance of the data dump -- way too slow:
  //Create a single dataframe from the responses and then write that to disk.
  //Use partition in the streamwriter to partition by timestamp, instead of manually creating the directory.



// COMMAND ----------

//dbutils.fs.rm("/FileStore/raw/Pokemon_Pseudonymised/",true)
//dbutils.fs.rm("/FileStore/raw/Pokemon_Identifier/",true)


// COMMAND ----------


