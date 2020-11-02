def flatten(df, path: list, dtype: str):
    """Flatten a document based on a path."""
    if len(path) < 2:
        raise ValueError("path must be at least two elements long")

    sub = df
    # walk the tree
    for i in range(len(path) - 1):
        key = path[i]
        subkey = path[i + 1]
        sub = (
            sub.select(F.col("_name").alias(key), F.explode("imgdir").alias("_tmp"))
            .select(key, "_tmp.*")
            .where(F.col("_name") == subkey)
        )
    is_array = sub.schema[dtype].dataType.typeName() == "array"
    subdoc = sub.withColumn("_tmp", F.explode(dtype) if is_array else F.col(dtype))
    return subdoc.select(key, "_tmp.*").groupBy(key).pivot("_name").agg(F.max("_value"))
