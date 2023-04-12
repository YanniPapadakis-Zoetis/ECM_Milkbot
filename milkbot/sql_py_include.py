# Databricks notebook source
import numpy as np
import yaml
from typing import List
from pyspark.sql.types import DoubleType, ArrayType, StringType
from scipy.optimize import curve_fit

# Load Configuration Parameter from YAML File
with open("milkbot/ecm_params.yml", "r") as f:
    param = yaml.safe_load(f)

# Create Spark Data Frame with Days from 0 to 349
# Result is available as SQL Temporary View GRAPHDIM
gdim = spark.createDataFrame([(i,) for i in range(350)], ["DIM"]).createOrReplaceTempView("GRAPHDIM")
  
# Milkbot Formula
def milkbot_(t: int, a: float, b: float, c: float, d: float) -> float:
    return a * (1 - np.exp((c - t)/b)/2) * np.exp(-d*t)

# Error Allowance
def error_allowance(dim_llim: int = 30, err_inflation: float = 1.5) -> float:
    return lambda dim: 1.0 if dim > dim_llim else err_inflation

# Estimate Milkbot Parameters Given t (DIM), y (ECM), and s (Error Sigma) Data
# Returns Estimated Parameters as List of 4 Float Numbers
# param["cfit"] contains p0: Initial Parameter List and bounds: [LowerBoundList, UpperBoundList]
def estimate_milkbot_params(t: List[float], y: List[float], s: List[float]) -> List[float]:
    popt, _ = curve_fit(milkbot_, t, y, sigma=s, **param["cfit"])
    return popt.tolist()

# Spark UDF for ECM Estimation Given DIM and Parameter List [a,b,c,d]
spark.udf.register("milkbot_est", lambda dim, par: float(milkbot_(dim, *par)), DoubleType())

# Spark UDF for Group Naming from Integer Code
# 0 -> Herd
# 1 -> Lactation Group 1
# 2 -> Lactation Group 2
# 3 -> Lactation Group 3
# 4 -> Lactation Group 4+
spark.udf.register("group_name", lambda i: param["hlgroups"][i], StringType())

# Transform Python Functions to Spark UDF
spark.udf.register("estimate_milkbot_params", estimate_milkbot_params, ArrayType(DoubleType()))
spark.udf.register("sigma_udf", error_allowance(), DoubleType());
