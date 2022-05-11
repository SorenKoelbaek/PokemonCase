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


val executeRestApiUDF = udf(new UDF1[String, String] {
  override def call(url: String) = {
    val httpRequest = new HttpRequest;
    httpRequest.ExecuteHttpGet(url).getOrElse("")
  }
}, StringType)


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
  var restApiCallsToMake = Seq(RestAPIRequest(uri));
  var source_df = restApiCallsToMake.toDF();
  val execute_df = source_df
  .withColumn("result", executeRestApiUDF(col("url")))
  .withColumn("result", from_json(col("result"), PokemonIteraterSchema))

  var nextUrl = execute_df.select(col("result.next")).first().getString(0);
  
//Explode the Pokemon result object alongside the attributed URL
  val delta_df =  execute_df.select(explode(col("result.Results")).alias("pokemons"))
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


//Now we loop through all pokemons in our Iterator-list to extract information on each pokemon and save that into our Raw filesystem alongside a watermark and extraction URL.
// Defining the pokemon response schema
val PokemonSchema = StructType(List(
  StructField("base_experience", IntegerType, true),
  StructField("id", IntegerType, true),
  StructField("order", IntegerType, true),
  StructField("name", StringType, true),
  StructField("weight", IntegerType, true),
  StructField("height", IntegerType, true),

))

case class pokemonUrl(name:String, url:String)
var pokemons_df = spark.createDataFrame(spark.sparkContext
      .emptyRDD[Row], PokemonSchema)

PokemonIterater_df.as[pokemonUrl].take(PokemonIterater_df.count.toInt).foreach(t => 
{
  val uri = t.url
  val name = t.name


  var restApiCallsToMake = Seq(RestAPIRequest(uri));
  var source_df = restApiCallsToMake.toDF();
  val execute_df = source_df
  .withColumn("result", executeRestApiUDF(col("url")))
  .withColumn("result", from_json(col("result"), PokemonSchema))
  
  
  val delta_df =  execute_df.select(col("result.*"))
   
  pokemons_df = pokemons_df.union(delta_df);
})


pokemons_df.write.mode('Overwrite').json("/tmp/spark_output/zipcodes.json")


case class RestAPIRequest (url: String)

class HttpRequest {
   def ExecuteHttpGet(url: String) : Option[String] = {

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

    Option[String](result)
  }

 
}

// COMMAND ----------


