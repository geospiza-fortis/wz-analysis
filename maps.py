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
