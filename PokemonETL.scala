// Databricks notebook source
//Ingest JSON data into our DeltaLake using autoloader for a streambased ingestion pattern
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import io.delta.tables._
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}

val Pokemon_df = spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation","/FileStore/Schema/Pokemon")
      .option("cloudFiles.inferColumnTypes", "true")
      .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
      .load("/FileStore/raw/Pokemon/*/*.json")

//we add a ValidFrom and ValidTo values to our database tables
val EffStart = current_timestamp()
val EffEnd = java.sql.Timestamp.valueOf("9999-12-31 00:00:00")

val pokemons_updates_df = Pokemon_df
                           .withColumn("Identifier",sha2(col("id").cast(StringType),256))
                           .drop("name","id","species","forms")
                           .withColumn("ModifiedDate",EffStart)
                           .withColumn("ValidFrom",EffStart)
                           .withColumn("ValidTo",lit(EffEnd))
                           .withColumn("Current",lit(true))


val Pokemons_identifier_df = Pokemon_df
                             .select("name","id","species","forms")
                             .withColumn("Identifier",sha2(col("id").cast(StringType),256))  
                             .withColumn("ModifiedDate",EffStart)
                             .withColumn("ValidFrom",EffStart)
                             .withColumn("ValidTo",lit(EffEnd))
                             .withColumn("Current",lit(true))


val deltaTablePokemons = DeltaTable.forName("sourcePokemon")


//Bug: When we receive changed information -- we only mark the Old ones as non-current, we are not including the new rows :( So either non-matched or if match
// -- Possible solution is to union our updated to itself without a match, but still insert them with the mergekey - Argh! (That will bring me closer to the sample though)

//We do a deltatable merge operation to ensure that matched pokemon, while new in the landingzone are only updated if their values differ. We also maintain our SCD2 rows here.
def upsertPokemonStream(streamBatchDF: DataFrame, batchId: Long) {
  
   val updateDF = streamBatchDF
    .as("updates")
    .join(deltaTablePokemons.toDF.as("pokemons"),"Identifier")
    .where("pokemons.Current = true AND (pokemons.abilities <> updates.abilities or pokemons.base_experience <> updates.base_experience or pokemons.game_indices <> updates.game_indices or pokemons.height <> updates.height or pokemons.held_items <> updates.held_items or pokemons.is_default <> updates.is_default or pokemons.location_area_encounters <> updates.location_area_encounters or pokemons.moves <> updates.moves or pokemons.order <> updates.order or pokemons.past_types <> updates.past_types or pokemons.sprites <> updates.sprites or pokemons.stats <> updates.stats or pokemons.types <> updates.types or pokemons.weight <> updates.weight )")
    .selectExpr("updates.*")
  
  val combinedSCD2DF = updateDF
    .selectExpr("NULL as mergeKey","updates.*")
    .unionByName(
        streamBatchDF.withColumn("Mergekey",col("Identifier"))
    )
    
  
  
  deltaTablePokemons
  .as("pokemons")
  .merge(
    combinedSCD2DF.as("updates"),
    "pokemons.Identifier = updates.mergeKey")
  .whenMatched("pokemons.Current = true AND (pokemons.abilities <> updates.abilities or pokemons.base_experience <> updates.base_experience or pokemons.game_indices <> updates.game_indices or pokemons.height <> updates.height or pokemons.held_items <> updates.held_items or pokemons.is_default <> updates.is_default or pokemons.location_area_encounters <> updates.location_area_encounters or pokemons.moves <> updates.moves or pokemons.order <> updates.order or pokemons.past_types <> updates.past_types or pokemons.sprites <> updates.sprites or pokemons.stats <> updates.stats or pokemons.types <> updates.types or pokemons.weight <> updates.weight )")
  .updateExpr(
     Map(
       "current" -> "false",
       "ValidTo" -> "updates.ValidFrom"
     ))
  .whenNotMatched()
    .insertExpr(
    Map(
      "Identifier" -> "updates.Identifier",
      "abilities" -> "updates.abilities",
      "base_experience" -> "updates.base_experience",
      "game_indices" -> "updates.game_indices",
      "height" -> "updates.height",
      "held_items" -> "updates.held_items",
      "is_default" -> "updates.is_default",
      "location_area_encounters" -> "updates.location_area_encounters",
      "moves" -> "updates.moves",
      "order" -> "updates.order",
      "past_types" -> "updates.past_types",
      "sprites" -> "updates.sprites",
      "stats" -> "updates.stats",
      "types" -> "updates.types",
      "weight" -> "updates.weight",
      "date" -> "updates.date",
      "_rescued_data" -> "updates._rescued_data",
      "ModifiedDate" -> "updates.ModifiedDate",
      "current_timestamp" -> "updates.current_timestamp",
      "Current" -> "updates.Current",
      "ValidFrom" -> "updates.ValidFrom",
      "ValidTo" -> "updates.ValidTo",
      "ModifiedDate" -> "updates.ModifiedDate"
    ))
  .execute()
  
}


val deltaTablePokemons_identifier = DeltaTable.forName("sourcePokemon_Identifier")

def upsertPokemonIdentifiersStream(streamBatchDF: DataFrame, batchId: Long) {
  
  val updateDF = streamBatchDF
    .as("updates")
    .join(deltaTablePokemons_identifier.toDF.as("pokemonIdentifier"),"Identifier")
    .where("pokemonIdentifier.Current = true AND (pokemonIdentifier.name <> updates.name or pokemonIdentifier.id <> updates.id or pokemonIdentifier.species <> pokemonIdentifier.species or pokemonIdentifier.forms <> pokemonIdentifier.forms )")
    .selectExpr("updates.*")
  
  val combinedSCD2DF = updateDF
    .selectExpr("NULL as mergeKey","updates.*")
    .unionByName(
        streamBatchDF.withColumn("Mergekey",col("Identifier"))
    )
    
  
  deltaTablePokemons_identifier
  .as("pokemons")
  .merge(
    combinedSCD2DF.as("updates"),
    "pokemons.Identifier = updates.mergeKey")
  .whenMatched("pokemons.Current = true AND (pokemons.name <> updates.name or pokemons.id <> updates.id or pokemons.species <> updates.species or pokemons.forms <> updates.forms )")
  .updateExpr(
     Map(
       "current" -> "false",
       "ValidTo" -> "updates.ValidFrom",
       "ModifiedDate" -> "updates.ModifiedDate",
     ))
  .whenNotMatched()
  .insertExpr(
    Map(
      "Identifier" -> "updates.Identifier",
      "name" -> "updates.name",
      "id" -> "updates.id",
      "forms" -> "updates.forms",
      "species" -> "updates.species",
      "Current" -> "updates.Current",
      "ValidFrom" -> "updates.ValidFrom",
      "ValidTo" -> "updates.ValidTo",
      "ModifiedDate" -> "updates.ModifiedDate"
    )
  )
  .execute()
}



//We write our merge using foreachbatch to ensure our merge can happens in our streaming environment 
pokemons_updates_df.writeStream
  .format("delta")
  .foreachBatch(upsertPokemonStream _)
  .outputMode("update")
  .start()

Pokemons_identifier_df.writeStream
  .format("delta")
  .foreachBatch(upsertPokemonIdentifiersStream _)
  .outputMode("update")
  .start()



// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from sourcePokemon
// MAGIC union all
// MAGIC select count(*) from sourcePokemon_Identifier

// COMMAND ----------



// COMMAND ----------

//We do our Transformation between the bronze and Silver Layer, here we format and strip the information we need, while also adjusting our tables for the for the objects we need - we output to new database tables and maintain our logic for our SCD2.

import org.apache.spark.sql.SparkSession
import io.delta.implicits._
import org.apache.spark.sql.functions.{col, udf, from_json, explode, sha2, array_contains}

val appName: String = "PokemonCase"
val spark = SparkSession
  .builder()
  .appName(appName)
  .master("local[*]")
  .getOrCreate()


val pokemons_df = spark.readStream.format("delta").option("ignoreChanges",true).table("SourcePokemon") //In order to get updated rows as well as new one
val pokemonIdentifier_df = spark.readStream.format("delta").option("ignoreChanges",true).table("sourcePokemon_Identifier") //In order to get new rows!



val deltaTablePokemons = DeltaTable.forName("Pokemon")
val deltaTablePokemonsIdentifier = DeltaTable.forName("PokemonIdentifier")

//As we track our information in the bronze/extract layer, a change will always just have to be updated and new rows inserted
def upsertToPokemons(batchDF: DataFrame, batchId: Long) {
  deltaTablePokemons.as("pokemons")
    .merge(
      batchDF.as("updates"),
      "pokemons.Identifier = updates.Identifier AND pokemons.ValidFrom = updates.ValidFrom")
    .whenMatched().updateAll()
    .whenNotMatched().insertAll()
    .execute()
}


def upsertToPokemonIdentifiers(batchDF: DataFrame, batchId: Long) {
  deltaTablePokemonsIdentifier.as("pokemonIdentifier")
    .merge(
      batchDF.as("updates"),
      "pokemonIdentifier.Identifier = updates.Identifier AND pokemonIdentifier.ValidFrom = updates.ValidFrom")
    .whenMatched().updateAll()
    .whenNotMatched().insertAll()
    .execute()
} 



//We do our transformations and filters the pokemons
val FilteredPokemons_df =  pokemons_df
                 .filter(array_contains(col("game_indices.version.name"),"red") || array_contains(col("game_indices.version.name"),"blue") || array_contains(col("game_indices.version.name"),"leafgreen")  || array_contains(col("game_indices.version.name"),"white"))
    .select(
        $"Identifier"
       ,$"base_experience"
       ,$"order"
       ,$"weight"
       ,$"height"
       ,(($"weight"*10)/($"height"*10)).as("BMI")
       ,($"types.type.name")(0).as("Type1")
       ,($"types.type.name")(1).as("Type2")
       ,$"sprites.front_default"
       ,($"game_indices.version.name").as("Games")
       ,$"ValidFrom"
       ,$"ValidTo"
       ,$"Current"
    )



//We are also including our Name and ID data source, which would be subject to deletion if we needed to strip PII information from the data source.
//As we are filtering our data on the Games entity, we simply use a join function on our hash value to filter our Identifier information table.
val FilteredpokemonIdentifier_df = pokemonIdentifier_df
      .select(
       pokemonIdentifier_df("Identifier")
      ,pokemonIdentifier_df("id")
      ,pokemonIdentifier_df("name")
      ,pokemonIdentifier_df("ValidFrom")
      ,pokemonIdentifier_df("ValidTo")
      ,pokemonIdentifier_df("Current")
    )
    .withColumn("name",initcap(col("name")))
    




FilteredPokemons_df.writeStream
    .format("delta")
    .foreachBatch(upsertToPokemons _)
    .outputMode("update")
    .start()

  FilteredpokemonIdentifier_df.writeStream
    .format("delta")
    .foreachBatch(upsertToPokemonIdentifiers _)
    .outputMode("update")
    .start()






// COMMAND ----------





// COMMAND ----------

//(FilteredPokemons_df.writeStream.format("delta")
//       .option("trigger.once",true)
//      .option("checkpointLocation","FileStore/Schema/Pokemon")
//       .table("Pokemon")
//        )
// (FilteredpokemonIdentifier_df.writeStream.format("delta")
//       .option("trigger.once",true)
//       .option("checkpointLocation","FileStore/Schema/PokemonIdentifier")
//       .table("PokemonIdentifier")
//        )

// COMMAND ----------

//Lastly we take our transformed tables and outputs them into our reporting layer as dimensional objects, ready for consumption by our repotring engine.

//Define our dimensions and the fact table for the reporting

// COMMAND ----------

// MAGIC %sql
// MAGIC --Clean if need to rerun
// MAGIC --drop table if exists sourcePokemon_Identifier;
// MAGIC --drop table if exists sourcePokemon;
// MAGIC --drop table if exists PokemonIdentifier;
// MAGIC --drop table if exists Pokemon;

// COMMAND ----------

//Clean if need to rerun
//dbutils.fs.rm("FileStore/Schema/PokemonIdentifier/",true)
//dbutils.fs.rm("FileStore/Schema/Pokemon/",true)


// COMMAND ----------

// MAGIC %sql
// MAGIC Select * from sourcepokemon

// COMMAND ----------


