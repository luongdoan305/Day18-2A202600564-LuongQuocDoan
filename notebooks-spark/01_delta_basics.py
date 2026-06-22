# ---
# jupyter:
#   jupytext:
#     formats: py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# # NB1 — Delta Lake Basics
#
# **Mục tiêu:** Tạo Delta table, observe transaction log, demo schema enforcement.
#
# Maps to slide §2 (Delta Lake) + deliverable bullet 1.

# %%
import sys
sys.path.append("/workspace/scripts")
from spark_session import get_spark

spark = get_spark("nb1_delta_basics")


def list_delta_log_files(delta_path):
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs_path = spark._jvm.org.apache.hadoop.fs.Path(delta_path + "/_delta_log")
    fs = fs_path.getFileSystem(hadoop_conf)
    return sorted(
        status.getPath().toString()
        for status in fs.listStatus(fs_path)
        if status.getPath().getName().endswith(".json")
    )

# %% [markdown]
# ## 1. Write a Delta table

# %%
data = [
    (1, "alice", 30, "Hanoi"),
    (2, "bob", 25, "HCMC"),
    (3, "charlie", 35, "Danang"),
]
df = spark.createDataFrame(data, ["id", "name", "age", "city"])
table_path = "s3a://lakehouse/users_delta"
df.write.format("delta").mode("overwrite").save(table_path)

# %% [markdown]
# ## 2. Read it back + inspect transaction log
#
# Open MinIO console (http://localhost:9001) → `lakehouse/users_delta/_delta_log/`.
# You should see `00000000000000000000.json`.

# %%
spark.read.format("delta").load(table_path).show()
spark.sql(f"DESCRIBE HISTORY delta.`{table_path}`").show(truncate=False)

log_files = list_delta_log_files(table_path)
print("_delta_log JSON files:")
for log_file in log_files:
    print(f"  {log_file}")

assert log_files, "Expected at least one Delta transaction log JSON file"

# %% [markdown]
# ## 3. Schema enforcement — try to write a wrong schema

# %%
try:
    bad = spark.createDataFrame([(4, "dan", "thirty", "Hue")], ["id", "name", "age", "city"])
    bad.write.format("delta").mode("append").save(table_path)
    raise AssertionError("Schema enforcement failed: bad write unexpectedly succeeded")
except Exception as e:
    print("BLOCKED by schema enforcement (expected):")
    print(type(e).__name__, str(e)[:200])

# %% [markdown]
# ## 4. Schema evolution (opt-in)

# %%
new_col = spark.createDataFrame(
    [(4, "dan", 28, "Hue", "premium")],
    ["id", "name", "age", "city", "tier"],
)
new_col.write.format("delta").mode("append").option("mergeSchema", "true").save(table_path)
users = spark.read.format("delta").load(table_path).orderBy("id")
users.show()

assert "tier" in users.columns, "Expected mergeSchema to add tier"
assert users.where("id = 4").select("tier").first()[0] == "premium"
print("mergeSchema added the `tier` column (expected)")

# %% [markdown]
# ## ✅ Deliverable check
# - [ ] `_delta_log/` contains JSON files
# - [ ] Schema enforcement blocked the bad write
# - [ ] mergeSchema added the `tier` column

# %%
spark.stop()
