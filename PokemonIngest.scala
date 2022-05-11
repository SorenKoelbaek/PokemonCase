// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.functions.{col, udf, from_json, explode}
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

//We declare our schema to enfore our extraction
  //Types
val PokemonType = (new StructType)
        .add("name", StringType)
        .add("url", StringType)

val PokemonTypeSlots = (new StructType)
        .add("slot", IntegerType)
        .add("type", PokemonType)

val PokemonTypes = new ArrayType(PokemonTypeSlots, false)

  //Games
val PokemonGameVersion = (new StructType)
        .add("name", StringType)
        .add("url", StringType)

val PokemonGame = (new StructType)
        .add("game_index", IntegerType)
        .add("version", PokemonGameVersion)

val PokemonGames = new ArrayType(PokemonGame, false)
  
  //Sprites
val PokemonSprites = (new StructType)
        .add("front_default", StringType)



val PokemonSchema: StructType = (new StructType)
  .add("types", PokemonTypes)
  .add("game_indices", PokemonGames)
  .add("sprites", PokemonSprites)
  .add("base_experience", IntegerType)
  .add("weight", IntegerType)
  .add("height", IntegerType)
  .add("order", IntegerType)

//Hash these to support Pseudonymisation of data
  .add("id", IntegerType)
  .add("name", StringType)


case class pokemonUrl(name:String, url:String)
var pokemons_df = spark.createDataFrame(spark.sparkContext
      .emptyRDD[Row], PokemonSchema)

//Painfully slow!
//--> Maybe try a map or other type that natively supports iteration instead

PokemonIterater_df.as[pokemonUrl].take(PokemonIterater_df.count.toInt).foreach(t => 
{
 val uri = t.url
 val name = t.name

 val httpRequest = new HttpRequest;
 val source_df = spark.read.schema(PokemonSchema).json(Seq(httpRequest.ExecuteHttpGet(uri)).toDS())
  
 pokemons_df = pokemons_df.union(source_df);
})

pokemons_df.cache()

// COMMAND ----------


