// Databricks notebook source
//Ingest JSON data into our DeltaLake using autoloader for a streambased ingestion pattern
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import io.delta.tables._

val Pokemon_df = spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation","/FileStore/test/Pokemon")
      .option("cloudFiles.inferColumnTypes", "true")
      .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
      .load("/FileStore/raw/Pokemon/*/*.json")

//we add a ValidFrom and ValidTo values to our database tables
val EffStart = current_timestamp()
val EffEnd = java.sql.Timestamp.valueOf("9999-12-31 00:00:00")

val pokemons_updates_df = Pokemon_df
                           .filter(array_contains(col("game_indices.version.name"),"red") || array_contains(col("game_indices.version.name"),"blue") || array_contains(col("game_indices.version.name"),"leafgreen")  || array_contains(col("game_indices.version.name"),"white"))
                           .withColumn("Identifier",sha2(col("name"),256))
                           .drop("name","id","species","forms")
                           .withColumn("ModifiedDate",EffStart)
                           .withColumn("ValidFrom",EffStart)
                           .withColumn("ValidTo",lit(EffEnd))
                           .withColumn("Current",lit(true))


val Pokemons_identifier_df = Pokemon_df
                              .filter(array_contains(col("game_indices.version.name"),"red") || array_contains(col("game_indices.version.name"),"blue") || array_contains(col("game_indices.version.name"),"leafgreen")  || array_contains(col("game_indices.version.name"),"white"))
                             .select("name","id","species","forms")
                             .withColumn("Identifier",sha2(col("name"),256))  
                             .withColumn("ModifiedDate",EffStart)
                             .withColumn("ValidFrom",EffStart)
                             .withColumn("ValidTo",lit(EffEnd))
                             .withColumn("Current",lit(true))


val deltaTablePokemons = DeltaTable.forName("sourcePokemon")


//We do a deltatable merge operation to ensure that matched pokemon, while new in the landingzone are only updated if their values differ. We also maintain our SCD2 rows here.
def upsertPokemonStream(streamBatchDF: DataFrame, batchId: Long) {
  deltaTablePokemons
  .as("pokemons")
  .merge(
    streamBatchDF.as("updates"),
    "pokemons.Identifier = updates.Identifier")
  .whenMatched("pokemons.Current = true AND (pokemons.abilities <> updates.abilities or pokemons.base_experience <> updates.base_experience or pokemons.game_indices <> updates.game_indices or pokemons.height <> updates.height or pokemons.held_items <> updates.held_items or pokemons.is_default <> updates.is_default or pokemons.location_area_encounters <> updates.location_area_encounters or pokemons.moves <> updates.moves or pokemons.order <> updates.order or pokemons.past_types <> updates.past_types or pokemons.sprites <> updates.sprites or pokemons.stats <> updates.stats or pokemons.types <> updates.types or pokemons.weight <> updates.weight )")
  .updateExpr(
     Map(
       "current" -> "false",
       "ValidTo" -> "updates.ValidFrom"
     ))
  .whenNotMatched().insertAll()
  .execute()
}


val deltaTablePokemons_identifier = DeltaTable.forName("sourcePokemon_Identifier")

def upsertPokemonIdentifiersStream(streamBatchDF: DataFrame, batchId: Long) {
  deltaTablePokemons_identifier
  .as("pokemons")
  .merge(
    streamBatchDF.as("updates"),
    "pokemons.Identifier = updates.Identifier")
  .whenMatched("pokemons.Current = true AND (pokemons.name <> updates.name or pokemons.id <> updates.id or pokemons.species <> updates.species or pokemons.forms <> updates.forms )")
  .updateExpr(
     Map(
       "current" -> "false",
       "ValidTo" -> "updates.ValidFrom"
     ))
  .whenNotMatched().insertAll()
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
// MAGIC select count(*) from sourcePokemon union all
// MAGIC select count(*) from sourcePokemon_Identifier

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


val pokemons_df = spark.readStream.format("delta").table("SourcePokemon")
val pokemonIdentifier_df = spark.readStream.format("delta").table("sourcePokemon_Identifier")

val deltaTablePokemons = DeltaTable.forName("Pokemon")

def upsertToPokemons(batchDF: DataFrame, batchId: Long) {
  deltaTablePokemons.as("pokemons")
    .merge(
      batchDF.as("updates"),
      "pokemons.Identifier = updates.Identifier AND pokemons.Current = updates.Current")
    .whenMatched().updateAll()
    .whenNotMatched().insertAll()
    .execute()
}

val deltaTablePokemonsIdentifier = DeltaTable.forName("PokemonIdentifier")

def upsertToPokemonIdentifiers(batchDF: DataFrame, batchId: Long) {
  deltaTablePokemonsIdentifier.as("pokemonIdentifier")
    .merge(
      batchDF.as("updates"),
      "pokemonIdentifier.Identifier = updates.Identifier AND pokemonIdentifier.Current = updates.Current")
    .whenMatched().updateAll()
    .whenNotMatched().insertAll()
    .execute()
}

//We do our transformations and filters the pokemons
val FilteredPokemons_df =  pokemons_df
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

// MAGIC %sql
// MAGIC select count(*) from Pokemon union all
// MAGIC select count(*) from PokemonIdentifier

// COMMAND ----------

//Lastly we take our transformed tables and outputs them into our reporting layer as dimensional objects, ready for consumption by our repotring engine.

// COMMAND ----------

// MAGIC %sql
// MAGIC --Clean if need to rerun
// MAGIC --drop table if exists sourcePokemon_Identifier;
// MAGIC --drop table if exists sourcePokemon;
// MAGIC --drop table if exists PokemonIdentifier;
// MAGIC --drop table if exists Pokemon;

// COMMAND ----------

//Clean if need to rerun
//dbutils.fs.rm("/FileStore/Schema/",true)

// COMMAND ----------


