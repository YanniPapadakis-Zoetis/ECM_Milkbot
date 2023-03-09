import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit
from sklearn.metrics import mean_squared_error
import seaborn as sns
import sys
from milkbot.formulas import ecm_milk, milkbot_, cov2corr
sns.set_style("darkgrid", {"axes.facecolor": ".9"})


df = pd.read_csv("docs/cow1_1183.csv")
df['ECM'] = df.apply(lambda x: ecm_milk(x.MILK, x.PCTF, x.PCTP), axis=1)

df0 = df.loc[df.BDAT == '2002-04-30']
for v in ["ECM","MILK","PCTF","PCTP"]:
    sns.scatterplot(data=df0, x="DIM", y=v, hue="LACT", palette="bright")
    plt.savefig("docs/cow1_{}_v_DIM_cow1.png".format(v))
    plt.close()

df['SIGMA'] = df["DIM"].apply(lambda x: 1 if x > 60 else 1.5)

par_init = np.array([90.0, 100.0, -15.0, 0.002])
bounds = (
    [0., 0., -250., 0.], 
    [200., 100., 1000., 0.01]
    )
dim = np.arange(500)

res = []
idx = []
npoints = []
rmse = []

for g, grp in df.groupby(['BDAT','LACT']):
    if len(grp) < 4:
        print(g)
        print(grp)
        continue
    p0 = par_init
    p0[0] = grp.ECM.max()
    #popt, pcovlm, infodict, mesg, ier = curve_fit(milkbot_, grp.DIM, grp.ECM, p0=p0, sigma=grp.SIGMA, full_output=True)
    popt, _ = curve_fit(milkbot_, grp.DIM, grp.ECM, p0=p0, sigma=grp.SIGMA, bounds=bounds)
    #print(mesg)
    #print(infodict['nfev'], len(grp))
    #print(popt)
    res.append(popt)
    idx.append(g)
    npoints.append(len(grp))
    rmse.append(np.sqrt(mean_squared_error(grp.ECM, milkbot_(grp.DIM, *popt))))

res = pd.DataFrame(
    np.array(res),
    index=pd.MultiIndex.from_tuples(idx, names=["BDAT", "LACT"]),
    columns=list('abcd')
    )
res['npoints'] = npoints
res['rmse'] = rmse
res.to_csv('trf.csv')
print(res)

sns.pairplot(data=res)
plt.show()

