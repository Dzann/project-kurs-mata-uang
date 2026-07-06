import os
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

if os.name == "nt" and "HADOOP_HOME" not in os.environ:
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] = os.environ["HADOOP_HOME"] + r"\bin;" + os.environ["PATH"]

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, map_keys, to_timestamp, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, MapType, DoubleType
)

# =========================
# KONFIGURASI
# =========================
INPUT_DIR = "data/stream_input"          # folder yang ditulis producer.py
HISTORY_OUTPUT_DIR = "data/output/history"
LATEST_OUTPUT_DIR = "data/output/latest"
CHECKPOINT_DIR = "data/checkpoints"      # wajib untuk Structured Streaming

os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(HISTORY_OUTPUT_DIR, exist_ok=True)
os.makedirs(LATEST_OUTPUT_DIR, exist_ok=True)
os.makedirs(CHECKPOINT_DIR, exist_ok=True)


# =========================
# SCHEMA - HARUS SESUAI OUTPUT producer.py
# =========================
schema = StructType([
    StructField("source", StringType(), True),
    StructField("base", StringType(), True),
    StructField("date", StringType(), True),
    StructField("rates", MapType(StringType(), DoubleType()), True),
    StructField("ingestion_timestamp", StringType(), True),
])


def build_spark_session():
    spark = (
        SparkSession.builder
        .appName("CurrencyStreamingJob")
        .config("spark.sql.shuffle.partitions", "4")  # kecilkan, karena data volumenya ringan
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")  # kurangi log yang terlalu ramai
    return spark


def read_raw_stream(spark):
    """Baca folder JSON sebagai streaming source."""
    df = (
        spark.readStream
        .schema(schema)
        .option("maxFilesPerTrigger", 1)   # proses 1 file per trigger, biar terasa seperti streaming
        .json(INPUT_DIR)
    )
    return df


def transform(df):
    """
    Ubah struktur map 'rates' (mis. {"EUR": 0.92, "GBP": 0.79})
    menjadi baris-baris terpisah: base, currency, rate, event_time.

    Hasil akhir schema:
        base | currency | rate | event_date | ingestion_time
    """
    exploded = (
        df
        .withColumn("currency", explode(map_keys(col("rates"))))
        .withColumn("rate", col("rates")[col("currency")])
        .withColumn("ingestion_time", to_timestamp(col("ingestion_timestamp")))
        .select(
            col("base"),
            col("currency"),
            col("rate"),
            col("date").alias("event_date"),
            col("ingestion_time"),
        )
    )
    return exploded


def write_history(exploded_df):
    """
    Sink 1: simpan semua data historis dalam mode append.
    Dipakai nanti untuk chart tren/time-series di Streamlit.
    """
    query = (
        exploded_df.writeStream
        .format("parquet")
        .option("path", HISTORY_OUTPUT_DIR)
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "history"))
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )
    return query


def write_latest_snapshot(exploded_df):
    """
    Sink 2: setiap micro-batch, timpa (overwrite) file 'latest' dengan data
    dari batch tersebut saja. Streamlit tinggal baca file ini untuk
    menampilkan angka kurs paling baru tanpa perlu scan seluruh history.

    Pakai foreachBatch supaya bisa overwrite (bukan append) meski sumbernya stream.
    """
    def write_batch(batch_df, batch_id):
        if batch_df.count() == 0:
            return
        (
            batch_df
            .withColumn("batch_id", col("ingestion_time"))  # sekadar penanda, opsional
            .write
            .mode("overwrite")
            .parquet(LATEST_OUTPUT_DIR)
        )
        print(f"[Batch {batch_id}] Latest snapshot updated: {batch_df.count()} baris")

    query = (
        exploded_df.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "latest"))
        .trigger(processingTime="10 seconds")
        .start()
    )
    return query


def main():
    spark = build_spark_session()

    raw_stream = read_raw_stream(spark)
    exploded_stream = transform(raw_stream)

    # (Opsional, untuk debug) tampilkan juga ke console:
    # exploded_stream.writeStream.format("console").outputMode("append").start()

    history_query = write_history(exploded_stream)
    latest_query = write_latest_snapshot(exploded_stream)

    print("[INFO] Spark Structured Streaming job berjalan...")
    print(f"[INFO] Membaca dari : {INPUT_DIR}")
    print(f"[INFO] History sink : {HISTORY_OUTPUT_DIR}")
    print(f"[INFO] Latest sink  : {LATEST_OUTPUT_DIR}")
    print("[INFO] Tekan CTRL+C untuk berhenti.\n")

    # Tunggu semua query jalan sampai dihentikan manual
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()