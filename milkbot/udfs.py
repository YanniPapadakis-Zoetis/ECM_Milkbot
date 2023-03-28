from scipy.optimize import curve_fit

import pyspark.sql.functions as fn
from pyspark.sql.types import DoubleType, ArrayType, StringType

from milkbot import param
from milkbot.formulas import ecm_milk, milkbot_, error_allowance

# Translation of Python Functions to PySpark UDF
ecm_udf = fn.udf(ecm_milk, DoubleType())
sigma_udf = fn.udf(error_allowance(), DoubleType())
milkbot_est = fn.udf(lambda dim, par: float(milkbot_(dim, *par)), DoubleType())

def estimate_milkbot_params_ez(t, y, s):
    popt, _ = curve_fit(milkbot_, t, y, sigma=s, **param["cfit"])
    return popt.tolist()

milkbot_ez_udf = fn.udf(estimate_milkbot_params_ez, ArrayType(DoubleType()))

group_name_udf = fn.udf(lambda i: param["hlgroups"][i], StringType())
