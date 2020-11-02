from pathlib import Path
from pyspark.sql import SparkSession, functions as F, types as T, Window


def flatten(df, path: list, dtype: str):
    """Flatten a document based on a path."""
    if len(path) < 2:
        raise ValueError("path must be at least two elements long")
    sub = df
    # walk the tree
    for i in range(len(path) - 1):
        key = path[i]
        subkey = path[i + 1]
        # TODO: test nested 3 deep
        next_dtype = "imgdir" if i < len(path) else dtype
        sub = (
            sub.select(F.col("_name").alias(key), F.explode("imgdir").alias("_tmp"))
            .select(*path[: i + 1], "_tmp.*")
            .withColumn(subkey, F.col("_name"))
            .where(F.col(subkey) == subkey)
            .drop("_name", "_value_tag")
        )
        sub = sub.select(*(path[: i + 2] + [c for c in sub.columns if c not in path]))
    if dtype == "imgdir":
        return sub.select(*path, dtype)
    is_array = sub.schema[dtype].dataType.typeName() == "array"
    subdoc = sub.withColumn("_tmp", F.explode(dtype) if is_array else F.col(dtype))
    return (
        subdoc.select(*path, "_tmp.*").groupBy(key).pivot("_name").agg(F.max("_value"))
    )


def get_portals(df):
    k = ["root", "portal"]
    q = (
        flatten(df, k, "imgdir")
        .select(*k, F.explode("imgdir").alias("_tmp"))
        .select(*k, F.col("_tmp._name").alias("i"), "_tmp.*")
        .drop("_name", "_value_tag")
        .select(*k, "i", F.explode("int").alias("_tmp"))
        .where(F.col("_tmp._name") == "tm")
        # what is tm?
        .select(
            *k,
            F.collect_list("_tmp._value")
            .over(Window.partitionBy(*k).orderBy("i"))
            .alias("tm"),
        )
        .groupby(*k)
        .agg(F.max("tm").alias("tm"))
        .select(F.split(F.col("root"), "\.")[0].alias("src"), F.col("tm").alias("dst"))
    )
    return q


if __name__ == "__main__":
    wzpath = "../../wzexports"
    list(Path(wzpath).glob("*"))
    spark = SparkSession.builder.getOrCreate()
    df = (
        spark.read.format("xml")
        .options(rowTag="imgdir", valueTag="_value_tag")
        .load(f"{wzpath}/Map.wz/Map/Map0/*.xml")
    )

    portals = (
        get_portals(df)
        .select("src", F.explode("dst").alias("dst"))
        .groupBy("src", "dst")
        .agg(F.count("*").alias("weight"))
    )

    portals.drop("weight").where(
        "src <> 999999999 and dst <> 999999999"
    ).toPandas().to_csv("../data/processed/maps.csv", index=False, header=False)

    # string data
    df = (
        spark.read.format("xml")
        .options(rowTag="imgdir", valueTag="_value_tag")
        .load(f"{wzpath}/String.wz/Map.img.xml")
    )

    q = (
        flatten(df, ["root", "maple"], "imgdir")
        .select(F.explode("imgdir"))
        .selectExpr("col._name as uid", "explode(col.string)")
        .groupBy("uid")
        .pivot("col._name")
        .agg(F.max("col._value"))
    )

    (
        q.select("uid", "mapName", "streetName")
        .toPandas()
        .to_csv("../data/processed/mapsLabels.csv", index=False, header=False)
    )

    (
        portals.join(q.selectExpr("uid as src"), how="right")
        .join(q.selectExpr("uid as dst", how="right"))
        .toPandas()
        .to_csv("../data/processed/maps.csv", index=False, header=False)
    )