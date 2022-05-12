// Databricks notebook source
//Ingest JSON data into our DeltaLake using autoloader for a streambased ingestion pattern
import org.apache.spark.sql.functions.{current_timestamp, lit}   

val Pokemon_Identifier_df = spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation","/FileStore/test/Pokemon_Identifier")
      .option("cloudFiles.inferColumnTypes", "true")
      .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
      .load("/FileStore/raw/Pokemon_Identifier/*/*.json")

val Pokemon_Pseudonymised_df = spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation","/FileStore/test/Pokemon_Pseudonymised")
      .option("cloudFiles.inferColumnTypes", "true")
      .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
      .load("/FileStore/raw/Pokemon_Pseudonymised/*/*.json")

//we add a ValidFrom and ValidTo values to our database tables
val EffStart = current_timestamp()
val EffEnd = java.sql.Timestamp.valueOf("9999-12-31 00:00:00")

//Do we really need to truncate in our streaming setup, or can we make sure in our select we only take the newest???
sql("drop table if exists SourcePokemonIdentifier_temp")
sql("drop table if exists SourcePokemons_temp")

//we write our datestream
(Pokemon_Identifier_df
       .withColumn("ModifiedDate",EffStart)
       .withColumn("ValidFrom",EffStart)
       .withColumn("ValidTo",lit(EffEnd))
       .withColumn("Current",lit(true))
       .writeStream.format("delta")
       .option("trigger.once",true)
       .option("checkpointLocation","FileStore/test/Pokemon_Identifier")
       .table("SourcePokemonIdentifier_temp")
        )
 (Pokemon_Pseudonymised_df
       .withColumn("ModifiedDate",EffStart)
       .withColumn("ValidFrom",EffStart)
       .withColumn("ValidTo",lit(EffEnd))
       .withColumn("Current",lit(true))
       .writeStream.format("delta")
       .option("trigger.once",true)
       .option("checkpointLocation","FileStore/test/Pokemon_Pseudonymised")
       .table("SourcePokemons_temp")
        )
 

// COMMAND ----------

// MAGIC %sql
// MAGIC --Merge the delta with our current data, checking for updates, and if so updating the SCD2 information in our tables
// MAGIC 
// MAGIC MERGE INTO SourcePokemons
// MAGIC 
// MAGIC USING (
// MAGIC 
// MAGIC   SELECT SourcePokemons_temp.Identifier as mergeKey, SourcePokemons_temp.*
// MAGIC   FROM SourcePokemons_temp
// MAGIC 
// MAGIC   UNION ALL
// MAGIC   -- These rows will INSERT new addresses of existing customers 
// MAGIC 
// MAGIC   -- Setting the mergeKey to NULL forces these rows to NOT MATCH and be INSERTed.
// MAGIC 
// MAGIC   SELECT NULL as mergeKey, SourcePokemons_temp.*
// MAGIC 
// MAGIC   FROM SourcePokemons_temp JOIN SourcePokemons
// MAGIC 
// MAGIC   ON SourcePokemons_temp.Identifier = SourcePokemons.Identifier 
// MAGIC 
// MAGIC   WHERE SourcePokemons.Current = true 
// MAGIC         AND 
// MAGIC          (SourcePokemons.abilities <> SourcePokemons_temp.abilities or
// MAGIC           SourcePokemons.base_experience <> SourcePokemons_temp.base_experience or
// MAGIC           SourcePokemons.game_indices <> SourcePokemons_temp.game_indices or
// MAGIC           SourcePokemons.height <> SourcePokemons_temp.height or
// MAGIC           SourcePokemons.held_items <> SourcePokemons_temp.held_items or
// MAGIC           SourcePokemons.is_default <> SourcePokemons_temp.is_default or
// MAGIC           SourcePokemons.location_area_encounters <> SourcePokemons_temp.location_area_encounters or
// MAGIC           SourcePokemons.order <> SourcePokemons_temp.order or
// MAGIC           SourcePokemons.past_types <> SourcePokemons_temp.past_types or
// MAGIC           SourcePokemons.sprites <> SourcePokemons_temp.sprites or
// MAGIC           SourcePokemons.stats <> SourcePokemons_temp.stats or
// MAGIC           SourcePokemons.types <> SourcePokemons_temp.types or
// MAGIC           SourcePokemons.weight <> SourcePokemons_temp.weight)
// MAGIC 
// MAGIC ) staged_pokemons
// MAGIC 
// MAGIC ON SourcePokemons.Identifier = mergeKey
// MAGIC WHEN MATCHED 
// MAGIC   AND SourcePokemons.Current = true 
// MAGIC   AND 
// MAGIC      (SourcePokemons.abilities <> staged_pokemons.abilities or
// MAGIC       SourcePokemons.base_experience <> staged_pokemons.base_experience or
// MAGIC       SourcePokemons.game_indices <> staged_pokemons.game_indices or
// MAGIC       SourcePokemons.height <> staged_pokemons.height or
// MAGIC       SourcePokemons.held_items <> staged_pokemons.held_items or
// MAGIC       SourcePokemons.is_default <> staged_pokemons.is_default or
// MAGIC       SourcePokemons.location_area_encounters <> staged_pokemons.location_area_encounters or
// MAGIC       SourcePokemons.order <> staged_pokemons.order or
// MAGIC       SourcePokemons.past_types <> staged_pokemons.past_types or
// MAGIC       SourcePokemons.sprites <> staged_pokemons.sprites or
// MAGIC       SourcePokemons.stats <> staged_pokemons.stats or
// MAGIC       SourcePokemons.types <> staged_pokemons.types or
// MAGIC       SourcePokemons.weight <> staged_pokemons.weight)
// MAGIC     THEN  
// MAGIC   UPDATE 
// MAGIC     SET Current = false, ValidTo = staged_pokemons.ValidFrom
// MAGIC WHEN NOT MATCHED THEN 
// MAGIC   INSERT *;
// MAGIC   
// MAGIC MERGE INTO SourcePokemonIdentifier
// MAGIC 
// MAGIC USING (
// MAGIC 
// MAGIC   SELECT SourcePokemonIdentifier_temp.Identifier as mergeKey, SourcePokemonIdentifier_temp.*
// MAGIC   FROM SourcePokemonIdentifier_temp
// MAGIC 
// MAGIC   UNION ALL
// MAGIC   -- These rows will INSERT new addresses of existing customers 
// MAGIC 
// MAGIC   -- Setting the mergeKey to NULL forces these rows to NOT MATCH and be INSERTed.
// MAGIC 
// MAGIC   SELECT NULL as mergeKey, SourcePokemonIdentifier_temp.*
// MAGIC 
// MAGIC   FROM SourcePokemonIdentifier_temp JOIN SourcePokemonIdentifier
// MAGIC 
// MAGIC   ON SourcePokemonIdentifier_temp.Identifier = SourcePokemonIdentifier.Identifier 
// MAGIC 
// MAGIC   WHERE SourcePokemonIdentifier.Current = true 
// MAGIC         AND 
// MAGIC          (SourcePokemonIdentifier.forms <> SourcePokemonIdentifier_temp.forms or
// MAGIC           SourcePokemonIdentifier.id <> SourcePokemonIdentifier_temp.id or
// MAGIC           SourcePokemonIdentifier.name <> SourcePokemonIdentifier_temp.name or
// MAGIC           SourcePokemonIdentifier.species <> SourcePokemonIdentifier_temp.species
// MAGIC          )
// MAGIC 
// MAGIC ) staged_pokemonIdentifier
// MAGIC 
// MAGIC ON SourcePokemonIdentifier.Identifier = mergeKey
// MAGIC WHEN MATCHED 
// MAGIC   AND SourcePokemonIdentifier.Current = true 
// MAGIC   AND 
// MAGIC      (SourcePokemonIdentifier.forms <> staged_pokemonIdentifier.forms or
// MAGIC       SourcePokemonIdentifier.id <> staged_pokemonIdentifier.id or
// MAGIC       SourcePokemonIdentifier.name <> staged_pokemonIdentifier.name or
// MAGIC       SourcePokemonIdentifier.species <> staged_pokemonIdentifier.species)
// MAGIC     THEN  
// MAGIC   UPDATE 
// MAGIC     SET Current = false, ValidTo = staged_pokemonIdentifier.ValidFrom
// MAGIC WHEN NOT MATCHED THEN 
// MAGIC   INSERT *;

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


val pokemons_df = spark.readStream.format("delta").table("SourcePokemons")
val pokemonIdentifier_df = spark.readStream.format("delta").table("SourcePokemonIdentifier")


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
  .filter(
      array_contains(col("Games"),"red") || array_contains(col("Games"),"blue") || array_contains(col("Games"),"leafgreen ")  || array_contains(col("Games"),"white ") );

(FilteredPokemons_df
       .writeStream.format("delta")
       .option("trigger.once",true)
       .option("checkpointLocation","FileStore/test/Pokemon_standardised")
       .table("Pokemon")
        )



//We are also including our Name and ID data source, which would be subject to deletion if we needed to strip PII information from the data source.
//As we are filtering our data on the Games entity, we simply use a join function on our hash value to filter our Identifier information table.
val FilteredPokemonIdentifers_df =  pokemonIdentifier_df.join(FilteredPokemons_df,FilteredPokemons_df("Identifier") === pokemonIdentifier_df("Identifier"),"inner")
    .select(
        pokemonIdentifier_df("Identifier")
      ,pokemonIdentifier_df("name")
      ,pokemonIdentifier_df("id")
      ,pokemonIdentifier_df("ValidFrom")
      ,pokemonIdentifier_df("ValidTo")
      ,pokemonIdentifier_df("Current")
    );

(FilteredPokemonIdentifers_df
       .writeStream.format("delta")
       .option("trigger.once",true)
       .option("checkpointLocation","FileStore/test/Pokemon_Identifier_standardised")
       .table("PokemonIdentifier")
        )



// COMMAND ----------

//Lastly we take our transformed tables and outputs them into our reporting layer as dimensional objects, ready for consumption by our repotring engine.

// COMMAND ----------

// MAGIC %sql
// MAGIC --Clean if need to rerun
// MAGIC --drop table if exists SourcePokemonIdentifier;
// MAGIC --drop table if exists SourcePokemons;
// MAGIC --drop table if exists SourcePokemonIdentifier_temp;
// MAGIC --drop table if exists SourcePokemons_temp;

// COMMAND ----------

//Clean if need to rerun
//dbutils.fs.rm("/FileStore/test/",true)


// COMMAND ----------


