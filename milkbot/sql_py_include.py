# Databricks notebook source
import pyspark.sql.functions as fn
from pyspark.sql.types import DoubleType, ArrayType, StringType
from milkbot import param
from milkbot.formulas import milkbot_, error_allowance
from scipy.optimize import curve_fit
  
gdim = spark.createDataFrame([(i,) for i in range(0,350,5)], ["DIM"]).createOrReplaceTempView("GRAPHDIM")

def estimate_milkbot_params(t, y, s):
    popt, _ = curve_fit(milkbot_, t, y, sigma=s, **param["cfit"])
    return popt.tolist()

spark.udf.register("milkbot_est", lambda dim, par: float(milkbot_(dim, *par)), DoubleType())
spark.udf.register("group_name", lambda i: param["hlgroups"][i], StringType())
spark.udf.register("estimate_milkbot_params", estimate_milkbot_params, ArrayType(DoubleType()))
spark.udf.register("sigma_udf", error_allowance(), DoubleType());
