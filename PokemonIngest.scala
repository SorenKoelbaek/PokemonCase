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

// Lets set up an example test
// create the Dataframe to bind the UDF to
case class RestAPIRequest (url: String)
val uri = "https://pokeapi.co/api/v2/pokemon/1";
val restApiCallsToMake = Seq(RestAPIRequest(uri))
val source_df = restApiCallsToMake.toDF()



// Define the schema used to format the REST response.  This will be used by from_json
val restApiSchema = StructType(List(
  StructField("base_experience", IntegerType, true),
  StructField("id", IntegerType, true),
  StructField("order", IntegerType, true),
  StructField("name", StringType, true),
  StructField("weight", IntegerType, true),
  StructField("height", IntegerType, true),

))

// add the UDF column, and a column to parse the output to
// a structure that we can interogate on the dataframe
val execute_df = source_df
  .withColumn("result", executeRestApiUDF(col("url")))
  .withColumn("result", from_json(col("result"), restApiSchema))

execute_df.show()

// call an action on the Dataframe to execute the UDF
// process the results
execute_df.select(col("result.*"))
      .show


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


