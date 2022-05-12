// Databricks notebook source
//Ingest JSON data into our DeltaLake using autoloader for a streambased ingestionpattern


val Pokemon_Identifier_df = spark.readStream.format("cloudFiles")
     .option("cloudFiles.format", "json")
     .option("cloudFiles.schemaLocation","/FileStore/test/Pokemon_Identifier")
     .option("cloudFiles.inferColumnTypes", "true")
     .load("/FileStore/raw/Pokemon_Identifier/*/*.json")

val Pokemon_Pseudonymised_df = spark.readStream.format("cloudFiles")
     .option("cloudFiles.format", "json")
     .option("cloudFiles.schemaLocation","/FileStore/test/Pokemon_Pseudonymised")
     .option("cloudFiles.inferColumnTypes", "true")
     .load("/FileStore/raw/Pokemon_Pseudonymised/*/*.json")


(Pokemon_Identifier_df.writeStream.format("delta")
       .option("trigger.once",true)
       .option("checkpointLocation","FileStore/test/Pokemon_Identifier")
       .table("Pokemon_Identifier")
        )
 (Pokemon_Pseudonymised_df.writeStream.format("delta")
       .option("trigger.once",true)
       .option("checkpointLocation","FileStore/test/Pokemon_Pseudonymised")
       .table("Pokemon_Pseudonymised")
        )
 

// COMMAND ----------

// MAGIC %sql 
// MAGIC select count(*) from Pokemon_Identifier
// MAGIC union all 
// MAGIC select count(*) from Pokemon_Pseudonymised

// COMMAND ----------

//We do our Transformation between the bronze and Silver Layer, here we format and strip the information we need, while also adjusting our tables for the for the objects we need - we output to new database tables and maintain our logic for our SCD2.


// COMMAND ----------

//Lastly we take our transformed tables and outputs them into our reporting layer as dimensional objects, ready for consumption by our repoting engine.
