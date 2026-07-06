import os
import sys

# Pastikan Spark memakai Python yang sama dengan venv yang sedang aktif,
# supaya tidak terganggu oleh Windows App Execution Alias untuk "python".
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Fallback untuk HADOOP_HOME di Windows (dibutuhkan winutils.exe untuk
# operasi checkpoint & file listing Spark Structured Streaming).
if os.name == "nt" and "HADOOP_HOME" not in os.environ:
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] = os.environ["HADOOP_HOME"] + r"\bin;" + os.environ["PATH"]

import shutil
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lag, row_number
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# =========================
# KONFIGURASI
# =========================
HISTORY_DIR = "data/output/history"
MODEL_DIR = "models/rate_predictor"
PREDICTIONS_OUTPUT_DIR = "data/output/predictions"

MIN_ROWS_PER_CURRENCY = 10  # minimal data poin per currency biar training masuk akal


def build_spark_session():
    spark = (
        SparkSession.builder
        .appName("CurrencyModelTraining")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_history(spark):
    df = spark.read.parquet(HISTORY_DIR)
    return df


def build_features(df):

    window_spec = Window.partitionBy("currency").orderBy("ingestion_time")

    featured = (
        df
        .withColumn("rate_lag1", lag("rate", 1).over(window_spec))
        .withColumn("rate_lag2", lag("rate", 2).over(window_spec))
        .withColumn("row_num", row_number().over(window_spec))
        .dropna(subset=["rate_lag1", "rate_lag2"])  # buang baris awal yang belum ada lag-nya
    )
    return featured


def train_and_evaluate(spark, featured_df):

    total_count = featured_df.count()
    if total_count < MIN_ROWS_PER_CURRENCY:
        print(f"[WARNING] Data masih sedikit ({total_count} baris). "
              f"Tunggu producer.py + spark_streaming_job.py jalan lebih lama "
              f"lalu jalankan ulang script ini.")
        return None, None

    assembler = VectorAssembler(
        inputCols=["rate_lag1", "rate_lag2"],
        outputCol="features",
    )
    assembled = assembler.transform(featured_df)

    train_df, test_df = assembled.randomSplit([0.8, 0.2], seed=42)

    lr = LinearRegression(featuresCol="features", labelCol="rate")
    model = lr.fit(train_df)

    predictions = model.transform(test_df)

    evaluator = RegressionEvaluator(
        labelCol="rate", predictionCol="prediction", metricName="rmse"
    )
    rmse = evaluator.evaluate(predictions)
    print(f"[INFO] Training selesai. RMSE pada test set: {rmse:.6f}")

    return model, predictions


def save_model(model):
    if os.path.exists(MODEL_DIR):
        shutil.rmtree(MODEL_DIR)
    model.save(MODEL_DIR)
    print(f"[INFO] Model disimpan di: {MODEL_DIR}")


def save_predictions(predictions):
    output = predictions.select(
        "currency", "ingestion_time", "rate", "prediction"
    )
    output.write.mode("overwrite").parquet(PREDICTIONS_OUTPUT_DIR)
    print(f"[INFO] Hasil prediksi vs aktual disimpan di: {PREDICTIONS_OUTPUT_DIR}")


def main():
    spark = build_spark_session()

    print("[INFO] Membaca data historis...")
    history_df = load_history(spark)

    print("[INFO] Membangun fitur lag per currency...")
    featured_df = build_features(history_df)

    print("[INFO] Training model...")
    model, predictions = train_and_evaluate(spark, featured_df)

    if model is not None:
        save_model(model)
        save_predictions(predictions)
        print("[DONE] Training selesai.")
    else:
        print("[DONE] Training dilewati karena data belum cukup.")

    spark.stop()


if __name__ == "__main__":
    main()