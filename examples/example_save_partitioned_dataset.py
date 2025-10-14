# Setup
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import numpy as np
import polars as pl

# Create a dataset
table = pa.table(
    {
        "a": range(10),
        "b": np.random.randn(10),
        "c": [1, 2] * 5,
        "part": ["a"] * 5 + ["b"] * 5,
    }
)

# Write the dataset to a partitioned parquet dataset
pq.write_to_dataset(
    table,
    "parquet_dataset_partitioned",
    partition_cols=["part"],
    existing_data_behavior="delete_matching",
)

# Create a dataset from the partitioned parquet dataset
dataset = ds.dataset(
    "parquet_dataset_partitioned", format="parquet", partitioning="hive"
)

# List the files in the dataset
dataset.files

# Show the dataset head
pl.from_arrow(dataset.to_table()).head(3)

# Filter the dataset
pl.from_arrow(dataset.to_table(filter=ds.field("part") == "b"))
