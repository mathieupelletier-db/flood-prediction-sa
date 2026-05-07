# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Train Random Forest, register model, score per-scenario
# MAGIC
# MAGIC * Trains `RandomForestClassifier` on the **scenario-expanded** `gold_h3_training`
# MAGIC   set so the model learns the flood response to 24-hour rainfall.
# MAGIC * Logs and registers the model in Unity Catalog via MLflow.
# MAGIC * Scores every H3 cell at each discrete rainfall scenario and persists
# MAGIC   `gold_h3_flood_predictions` partitioned by `(aoi_name, scenario_24h_mm)` so
# MAGIC   the app can switch partitions on every slider move.

# COMMAND ----------

dbutils.widgets.text("catalog", "flood_demo")
dbutils.widgets.text("schema", "montreal")
dbutils.widgets.text("aoi_name", "greater_montreal")
dbutils.widgets.text("model_name", "flood_demo.montreal.flood_rf")
dbutils.widgets.text("scenarios_24h_mm", "10,30,60,100,150,200")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
aoi_name = dbutils.widgets.get("aoi_name")
model_name = dbutils.widgets.get("model_name")
scenarios = [int(s.strip()) for s in dbutils.widgets.get("scenarios_24h_mm").split(",") if s.strip()]
ns = f"{catalog}.{schema}"
print("Scenarios (mm/24h):", scenarios)

# COMMAND ----------

import mlflow
import mlflow.spark
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

mlflow.set_registry_uri("databricks-uc")

FEATURES = [
    "avg_elev", "min_elev", "slope_deg", "dist_to_water_m", "twi",
    "annual_precip_mm", "max24h_precip_mm", "max5d_precip_mm",
    "scenario_24h_mm",
]

train_base = (spark.table(f"{ns}.gold_h3_training")
                    .where(F.col("aoi_name") == aoi_name)
                    .na.drop(subset=FEATURES))
print("Training rows:", train_base.count())

train, test = train_base.randomSplit([0.8, 0.2], seed=42)

assembler = VectorAssembler(inputCols=FEATURES, outputCol="features")
rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="label_synthetic",
    numTrees=100,
    maxDepth=8,
    seed=42,
)
pipe = Pipeline(stages=[assembler, rf])

with mlflow.start_run(run_name=f"flood_rf_{aoi_name}") as run:
    model = pipe.fit(train)

    pred_test = model.transform(test)
    auc = BinaryClassificationEvaluator(
        labelCol="label_synthetic", rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    ).evaluate(pred_test)
    mlflow.log_metric("test_auc", auc)
    print(f"AUC on synthetic holdout: {auc:.3f}")

    # Feature importances
    rf_model = model.stages[-1]
    for name, imp in sorted(zip(FEATURES, rf_model.featureImportances.toArray()),
                            key=lambda x: -x[1]):
        mlflow.log_metric(f"importance__{name}", float(imp))
        print(f"  {name}: {imp:.4f}")

    from mlflow.models.signature import infer_signature
    sample_input = train.select(FEATURES).limit(10).toPandas()
    sample_pred = model.transform(train.limit(10)).select("prediction").toPandas()
    signature = infer_signature(sample_input, sample_pred)
    mlflow.spark.log_model(
        spark_model=model,
        artifact_path="model",
        registered_model_name=model_name,
        input_example=sample_input.head(3),
        signature=signature,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Score every H3 cell at every scenario

# COMMAND ----------

prob_pos = F.udf(lambda v: float(v[1]) if v is not None else None, DoubleType())

features = spark.table(f"{ns}.gold_h3_features").where(F.col("aoi_name") == aoi_name)
labels = spark.table(f"{ns}.gold_h3_labels").select("aoi_name", "h3", "label_real")

scen_df = spark.createDataFrame([(int(s),) for s in scenarios], ["scenario_24h_mm"])

score_input = (features.crossJoin(scen_df)
                        .join(labels, ["aoi_name", "h3"], "left")
                        .withColumn("scenario_24h_mm",
                                    F.col("scenario_24h_mm").cast("double")))

scored = model.transform(score_input)

publish = (scored
  .withColumn("flood_prob", prob_pos(F.col("probability")))
  .select("aoi_name", "h3", "scenario_24h_mm", "avg_elev", "min_elev",
          "slope_deg", "dist_to_water_m", "twi",
          "annual_precip_mm", "max24h_precip_mm", "max5d_precip_mm",
          "building_count", "residential_count",
          "flood_prob",
          F.coalesce(F.col("label_real"), F.lit(0)).cast("int").alias("label_real"))
  .withColumn("scenario_24h_mm", F.col("scenario_24h_mm").cast("int")))

publish.createOrReplaceTempView("v_scored")

final = spark.sql("""
  SELECT aoi_name, scenario_24h_mm, h3,
         h3_boundaryasgeojson(h3) AS geometry_geojson,
         avg_elev, min_elev, slope_deg, dist_to_water_m, twi,
         annual_precip_mm, max24h_precip_mm, max5d_precip_mm,
         building_count, residential_count,
         CAST(flood_prob * building_count    AS DOUBLE) AS expected_buildings_at_risk,
         CAST(flood_prob * residential_count AS DOUBLE) AS expected_residential_at_risk,
         flood_prob, label_real
  FROM v_scored
""")

(final.write.mode("overwrite")
      .option("overwriteSchema", "true")
      .option("delta.columnMapping.mode", "name")
      .partitionBy("aoi_name", "scenario_24h_mm")
      .saveAsTable(f"{ns}.gold_h3_flood_predictions"))

# Publish historical flood polygons + AOI metadata (same as before)
spark.sql(f"""
  CREATE OR REPLACE TABLE {ns}.gold_flood_events AS
  SELECT aoi_name, year, geom_geojson AS geometry_geojson
  FROM {ns}.bronze_flood_events
""")
spark.sql(f"""
  CREATE OR REPLACE TABLE {ns}.gold_aoi AS
  SELECT aoi_name,
         MIN(min_lon) AS min_lon, MIN(min_lat) AS min_lat,
         MAX(max_lon) AS max_lon, MAX(max_lat) AS max_lat
  FROM {ns}.bronze_dem_manifest
  GROUP BY aoi_name
""")

# Scenario registry - lets the app enumerate available rainfall partitions
scen_rows = [(aoi_name, int(s)) for s in scenarios]
spark.createDataFrame(scen_rows, ["aoi_name", "scenario_24h_mm"]) \
     .write.mode("overwrite").option("overwriteSchema", "true") \
     .saveAsTable(f"{ns}.gold_scenarios")

print("Partitions written:", spark.sql(
    f"SHOW PARTITIONS {ns}.gold_h3_flood_predictions").count())

display(spark.sql(f"""
  SELECT scenario_24h_mm,
         COUNT(*) AS cells,
         ROUND(AVG(flood_prob), 3) AS mean_prob,
         ROUND(AVG(CASE WHEN flood_prob > 0.5 THEN 1.0 ELSE 0.0 END), 3) AS high_risk_rate,
         SUM(label_real) AS real_flood_cells
  FROM {ns}.gold_h3_flood_predictions
  WHERE aoi_name = '{aoi_name}'
  GROUP BY scenario_24h_mm
  ORDER BY scenario_24h_mm
"""))
