import numpy as np
from scipy.optimize import curve_fit, OptimizeWarning
import warnings

# Milkbot Formula Using Numpy Array
def milkbot(par: np.array, DIM: np.array):
    a, b, c, d = par
    return a * (1 - np.exp((c - DIM)/b)/2) * np.exp(-d*DIM)

# Milkbot Formula
def milkbot_(t, a, b, c, d):
    return a * (1 - np.exp((c - t)/b)/2) * np.exp(-d*t)

# Error Allowance
def error_allowance(dim_llim = 30, err_inflation = 1.5):
    return lambda dim: 1.0 if dim > dim_llim else err_inflation

# International ECM correction equation
# ECM milk = (milk production * (0.383 * % fat + 0.242 * % protein + 0.7832) / 3.1138)
ecm_milk = lambda milk, pctf, pctp: milk * (0.383 * pctf + 0.242 * pctp + 0.7832) / 3.1138

# Estimate Milkbot Formula
def estimate_milkbot_params(t, y, s, p0, bounds):
  try:
    popt, _ = curve_fit(milkbot_, t, y, p0=p0, sigma=s, bounds=bounds)
  except RuntimeError:
    popt = np.array([0.0, 10.0, 0.0, 0.0])
  return popt

# Estimate Milkbot Formula with Status
def estimate_milkbot_params_w_status(t, y, s, p0, bounds):
  status = 0
  with warnings.catch_warnings():
    warnings.simplefilter("error")
    try:
      popt, _ = curve_fit(milkbot_, t, y, p0=p0, sigma=s, bounds=bounds)
    except RuntimeError:
      status = 1
      popt = np.array([0.0, 1.0, 0.0, 0.0])
    except OptimizeWarning:
      status = 2
  return popt, status


# Covariance to Correlation Matrix
def cov2corr(covmat):
    S = np.array(covmat)
    diag = np.sqrt(np.diag(np.diag(S)))
    gaid = np.linalg.inv(diag)
    return gaid @ S @ gaid
