import numpy as np
import pandas as pd
import time
import warnings

from scipy.optimize import curve_fit
from scipy.optimize import OptimizeWarning
from sklearn.metrics import mean_squared_error

from milkbot.formulas import ecm_milk, milkbot_

id_vars = ['BDAT','EarTag','LACT']
df = pd.read_csv("docs/herd1183.csv")
df.info()

# Impute PCTF, PCTP Values
df['PCTF'] = df.groupby(id_vars)['PCTF'].bfill().ffill()
df['PCTP'] = df.groupby(id_vars)['PCTP'].bfill().ffill()
df.info()

df['ECM'] = df.apply(lambda x: ecm_milk(x.MILK, x.PCTF, x.PCTP), axis=1)
df['SIGMA'] = df["DIM"].apply(lambda x: 1 if x > 60 else 1.5)

par_init = np.array([90.0, 100.0, -15.0, 0.002])
bounds = (
    [0., 1., -250., 0.], 
    [500., 500., 200., 0.01]
    )   

res = []
idx = []
npoints = []
rmse = []
elapsed = []

cases = 0
cases_with_insufficient_data = 0
cases_no_convergence = 0
cases_covariance_not_estimated = 0

for g, grp in df.groupby(id_vars):
    cases += 1
    if cases % 1000 == 0:
        print(cases, cases_with_insufficient_data, cases_no_convergence, cases_covariance_not_estimated)
    if len(grp) <= 4:
        cases_with_insufficient_data += 1
        continue
    t = time.process_time()
    p0 = par_init
    #p0[0] = grp.ECM.max()
    with warnings.catch_warnings():
        warnings.simplefilter("error", OptimizeWarning)
        try:
            popt, _ = curve_fit(milkbot_, grp.DIM, grp.ECM, p0=p0, sigma=grp.SIGMA, bounds=bounds)
        except RuntimeError as re:
            cases_no_convergence += 1
            continue
        except OptimizeWarning:
            print("<=4 points for estimation")
            cases_covariance_not_estimated += 1
    elapsed.append(time.process_time() - t)
    res.append(popt)
    idx.append(g)
    npoints.append(len(grp))
    ecm_est = milkbot_(grp.DIM, *popt)
    rmse.append(np.sqrt(mean_squared_error(grp.ECM, ecm_est)))
    
print("Cases With Insufficient Data ", cases_with_insufficient_data)
print("Cases Where Estimation Failed", cases_no_convergence)

res = pd.DataFrame(
    np.array(res),
    index=pd.MultiIndex.from_tuples(idx, names=id_vars),
    columns=list('abcd')
    )
res['npoints'] = npoints
res['rmse'] = rmse
res['processing_time'] = elapsed
res.to_csv('herd1183_stats.csv')