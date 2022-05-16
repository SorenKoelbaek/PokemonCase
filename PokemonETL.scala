// Databricks notebook source
// MAGIC %md # Pokemon streaming ETL Engine
// MAGIC ### The ETL notebook has 3 main sections with each clear defined responsibilities:
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC  1. **The Bronze level / Extraction Area**: Receive our data, and store the data in a table with SCD columns added: _Modified, Current, ValidFrom, ValidTo_ This is also where we split our data into a seperate table containing PII information (in this case; name, id, species and forms) --> if we enable Pseudonymization, these values will be overwritten in the data, and will trigger a an (SCD2 method) merge instead of the otherwise SCD2 method used for our datastorage. The two objects are joinable by a one-way hash value based on the id of the pokemon.
// MAGIC  2. **The Silver Level/ Transformation Area**: We read our data from the Source layer when available and apply our filtering and transformation logic here. In this case, we retain the two objects: Pokemons and Pokemons_identifier, but we calculate BMI and extract the information we want from the array structure of the source information.
// MAGIC  3. **The Gold Level / Loading area**: In this area we create the appropiate dimensional model tabels and split our stream into the respective tables with the needed granularity. We then load out data into our reporting tables and make sure they are joinable by a clean albeit arbitrary integer id field.

// COMMAND ----------

//Ingest JSON data into our DeltaLake using autoloader for a streambased ingestion pattern
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import io.delta.tables._
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}

val Pseudonymisation = false //This will convert the name of the pokemon to "PokemonName". A change to the "name" will be seen "downstream" as a back-in-time change and will conform to SCD1 instead of SCD2 --> changing back does not reintroduce names historically!

val Pokemon_df = spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation","/FileStore/Schema/Pokemon")
      .option("cloudFiles.inferColumnTypes", "true")
      .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
      .option("CloudFiles.includeExistingFiles","false")
      .load("/FileStore/raw/Pokemon/*/*.json")

//we add a ValidFrom and ValidTo values to our database tables
val Modified = current_timestamp()
val EffEnd = java.sql.Timestamp.valueOf("9999-12-31 00:00:00")

case class Pseudo(name: String, url: String)
var Species = Pseudo("PokemonSpecies","PokemonSpecies")

val Forms: Array[Pseudo] = Array(
    Pseudo("PokemonForms","PokemonForms")
)

val pokemons_updates_df = Pokemon_df
                           .withColumn("Identifier",sha2(col("id").cast(StringType),256))
                           .drop("name","id","species","forms") //We exclude the columns defines as PII
                           .withColumn("ModifiedDate",Modified)
                           .withColumn("ValidFrom",to_timestamp(regexp_replace($"current_timestamp","%3A",":"),"yyyy-MM-dd HH:mm:ss.SSS"))
                           .withColumn("ValidTo",lit(EffEnd))
                           .withColumn("Current",lit(true))
                            


val Pokemons_identifier_df = Pokemon_df
                             .withColumn("ValidFrom",to_timestamp(regexp_replace($"current_timestamp","%3A",":"),"yyyy-MM-dd HH:mm:ss.SSS"))
                             .select("name","id","species","forms","ValidFrom")
                             .withColumn("Identifier",sha2(col("id").cast(StringType),256))  
                             .withColumn("name",when(lit(Pseudonymisation) === "true","PokemonName").otherwise(col("name")))
                             .withColumn("id",when(lit(Pseudonymisation) === "true",0).otherwise(col("id")))
                             .withColumn("species",when(lit(Pseudonymisation) === "true",typedLit(Species)).otherwise(col("species")))
                             .withColumn("forms",when(lit(Pseudonymisation) === "true",typedLit(Forms)).otherwise(col("forms")))
                             .withColumn("ModifiedDate",Modified)
                             .withColumn("ValidTo",lit(EffEnd))
                             .withColumn("Current",lit(true))




val deltaTablePokemons = DeltaTable.forName("sourcePokemon")
val deltaTablePokemons_identifier = DeltaTable.forName("sourcePokemon_Identifier")


//We do a deltatable merge operation to ensure that matched pokemon, while new in the landingzone are only updated if their values differ. We also maintain our SCD2 rows here. we DO NOT mark missing as deleted.
//We do not keep a record on deletion in our snapshot: 
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
       "ValidTo" -> "updates.ValidFrom",
       "ModifiedDate" -> "updates.ModifiedDate"
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



def upsertPokemonIdentifiersStreamSCD1(streamBatchDF: DataFrame, batchId: Long) {
  
  deltaTablePokemons_identifier
  .as("pokemons")
  .merge(
    streamBatchDF.as("updates"),
    "pokemons.Identifier = updates.Identifier")
   .whenMatched("pokemons.name <> updates.name or pokemons.id <> updates.id or pokemons.species <> updates.species or pokemons.forms <> updates.forms" )
  .updateExpr(
     Map(
       "name" -> "updates.name",
       "id" -> "updates.id",
       "species" -> "updates.species",
       "forms" -> "updates.forms",
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


def upsertPokemonIdentifiersStreamSCD2(streamBatchDF: DataFrame, batchId: Long) {
  
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


//Depending on on our Pseudonymisation-state we either use the SCD1 or SCD2 method defined above
if( Pseudonymisation ){
Pokemons_identifier_df.writeStream
  .format("delta")
  .foreachBatch(upsertPokemonIdentifiersStreamSCD1 _)
  .outputMode("update")
  .start()

} else{
Pokemons_identifier_df.writeStream
  .format("delta")
  .foreachBatch(upsertPokemonIdentifiersStreamSCD2 _)
  .outputMode("update")
  .start()

}






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


val pokemons_df = spark.readStream.format("delta").option("ignoreChanges",true).table("SourcePokemon") //We use ignorechanges to make sure we get updated rows downstream
val pokemonIdentifier_df = spark.readStream.format("delta").option("ignoreChanges",true).table("sourcePokemon_Identifier") //We use ignorechanges to make sure we get updated rows downstream


//We do our transformations and filters the pokemons
val FilteredPokemons_df =  pokemons_df
                 .filter(array_contains(col("game_indices.version.name"),"red") || array_contains(col("game_indices.version.name"),"blue") || array_contains(col("game_indices.version.name"),"leafgreen")  || array_contains(col("game_indices.version.name"),"white"))
    .select(
        $"Identifier"
       ,$"base_experience"
       ,$"order"
       ,$"weight"
       ,$"height"
       ,(($"weight"/10)/(($"height"/10)*($"height"/10))).as("BMI")
       ,($"types.type.name")(0).as("Type1")
       ,($"types.type.name")(1).as("Type2")
       ,$"sprites.front_default"
       ,($"game_indices.version.name").as("Games")
       ,$"ValidFrom"
       ,$"ValidTo"
       ,$"Current"
    )



//We are also including our Name and ID data source, which would be subject to deletion if we needed to strip PII information from the data source.
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

//Lastly we take our transformed tables and outputs them into our reporting layer as dimensional objects, the main purpose here is to utilize the identity columns to maintain unique keys upon changes: ready for consumption by our repotring engine.

val pokemons_df = spark.readStream.format("delta").option("ignoreChanges",true).table("Pokemon") //We use ignorechanges to make sure we get updated rows downstream
val pokemonIdentifier_df = spark.readStream.format("delta").option("ignoreChanges",true).table("PokemonIdentifier") //We use ignorechanges to make sure we get updated rows downstream


//Create the Identity DF and write that to the delta-table.
val dimIdentity_df = pokemonIdentifier_df
    .select(
    col("name").as("Name"),
    col("id").as("Id"),
    col("Identifier"),
    col("ValidFrom").as("IdentityValidFrom"),
    col("ValidTo").as("IdentityValidTo"),
    col("Current").as("IsCurrent"))
    .filter($"name".isNotNull)
    .distinct()

val deltadimIdentity = DeltaTable.forName("dimIdentity")

def UpsertDimIdentity(batchDF: DataFrame, batchId: Long) {
  deltadimIdentity.as("dim")
    .merge(
      batchDF.as("updates"),
      "dim.Identifier = updates.Identifier AND dim.IdentityValidFrom = updates.IdentityValidFrom")
    .whenMatched()
     .updateExpr(
     Map(
       "Name" -> "updates.Name",
       "Id" -> "updates.Id",
       "IdentityValidTo" -> "updates.IdentityValidTo",
       "IsCurrent" -> "updates.IsCurrent"
     ))
    .whenNotMatched()
    .insertExpr(
     Map(
       "Name" -> "updates.Name",
       "Id" -> "updates.Id",
       "Identifier" -> "updates.Identifier",
       "IdentityValidFrom" -> "updates.IdentityValidFrom",
       "IdentityValidTo" -> "updates.IdentityValidTo",
       "IsCurrent" -> "updates.IsCurrent"
     ))
    .execute()
} 


dimIdentity_df.writeStream
    .format("delta")
    .foreachBatch(UpsertDimIdentity _)
    .outputMode("update")
    .start()



//Create the Type Dimension and write that to the delta-table.
val dimType_df = pokemons_df
        .select(
        col("Type1"),
        col("Type2"))
        .distinct()

val deltadimType= DeltaTable.forName("dimType")

def UpsertDimType(batchDF: DataFrame, batchId: Long) {

  deltadimType.as("dim")
    .merge(
      batchDF.as("updates"),
      $"dim.Type1" === $"updates.Type1" && when($"dim.Type2".isNull,"NAN").otherwise($"dim.Type2") === when($"updates.Type2".isNull,"NAN").otherwise($"updates.Type2"))
    .whenNotMatched()
    .insertExpr(
     Map(
       "Type1" -> "updates.Type1",
       "Type2" -> "updates.Type2"
     ))
    .execute()
} 


dimType_df.writeStream
    .format("delta")
    .foreachBatch(UpsertDimType _)
    .outputMode("update")
    .start()

//Create the Picture Dimension and write that to the delta-table.
val dimPicture_df = pokemons_df
        .select(
        col("front_default").as("PokemonPicture"))
        .distinct()

val deltadimPicture= DeltaTable.forName("dimPicture")

def UpsertDimPicture(batchDF: DataFrame, batchId: Long) {
 deltadimPicture.as("dim")
    .merge(
      batchDF.as("updates"),
      "dim.PokemonPicture = updates.PokemonPicture")
    .whenNotMatched()
    .insertExpr(
     Map(
       "PokemonPicture" -> "updates.PokemonPicture"
     ))
    .execute()
} 


dimPicture_df.writeStream
    .format("delta")
    .foreachBatch(UpsertDimPicture _)
    .outputMode("update")
    .start()


//Create the Fact object and write that to the delta-table.

val factPokemon_df = pokemons_df
                      .select(
                        col("Type1"),
                        col("Type2"),
                        col("front_default").as("PokemonPicture"),
                        col("Identifier"),
                        col("height").as("Height"),
                        col("weight").as("Weight"),
                        col("order").as("Order"),
                        col("BMI"),
                        col("base_experience").as("Base_Experience"),
                        col("ValidFrom").as("ValidFrom"),
                        col("ValidTo").as("ValidTo"),
                        col("Current").as("IsCurrent"))


          
          
val deltafact= DeltaTable.forName("factPokemon")

def LoadPokemon(batchDF: DataFrame, batchId: Long) {

  
  val FactBatch = batchDF
         .as("fact")
         .join(deltadimType.toDF.as("Type"),$"fact.Type1" === $"Type.Type1" && when($"fact.Type2".isNull,"NAN").otherwise($"fact.Type2") === when($"Type.Type2".isNull,"NAN").otherwise($"Type.Type2"))
        .join(deltadimPicture.toDF.as("Picture"),$"fact.PokemonPicture" === $"Picture.PokemonPicture")
        .join(deltadimIdentity.toDF.as("Identity"),$"fact.Identifier" === $"Identity.Identifier" && $"fact.ValidFrom" >= $"Identity.IdentityValidFrom" && $"fact.ValidFrom" <= $"Identity.IdentityValidTo")
  .selectExpr("fact.Identifier","fact.Height","fact.Weight","fact.Order","cast(fact.BMI as Decimal(10,5))","fact.Base_Experience","fact.ValidFrom","fact.ValidTo","fact.IsCurrent","Type.TypeID","Picture.PictureID","Identity.IdentityID")



  deltafact.as("fact")
    .merge(
      FactBatch.as("updates"),
      "fact.Identifier = updates.Identifier AND fact.ValidFrom = updates.ValidFrom")
    .whenMatched().updateAll()
    .whenNotMatched().insertAll()
    .execute()
} 


factPokemon_df.writeStream
    .format("delta")
    .foreachBatch(LoadPokemon _)
    .outputMode("update")
    .start()

          


