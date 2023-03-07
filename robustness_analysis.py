import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit
import seaborn as sns
from milkbot.formulas import ecm_milk, milkbot_, cov2corr

out = open("explore_minimization_methods.md", "w")

print("# Explore Minimization Methods for Milkbot Estimation Applied to ECM", file=out)

df = pd.read_csv("docs/example1.csv")
df['ECM_RAW'] = df.apply(lambda x: ecm_milk(x.MILK, x.PCTF, x.PCTP), axis=1)
meas_error = np.ones(len(df))
early = np.where(df['DIM'] < 60)[0]
meas_error[early] = 1.5


par_init = np.array([90.0, 100.0, -15.0, 0.002])
bounds = (
    [0., 0., -1000., 0.], 
    [200., 100., 1000., 0.01]
    )
dim = np.arange(500)

plt.plot(df.DIM, df.ECM_RAW, 'ro', label='data')

print("## Unconstrained Optimization: Levenberg-Marquardt Algorithm", file=out)
popt, pcovlm, infodict, mesg, ier = curve_fit(milkbot_, df.DIM, df.ECM_RAW, p0=par_init, sigma=meas_error, full_output=True)
print("\n- Milkbot Parameter Estimates a=%5.3f, b=%5.3f, c=%5.3f, d=%5.3f" % tuple(popt), file=out)
print("\n- Error Correlation Matrix", file=out)
print("\n![Error Correlations LM](docs/ecm_error_correlations_LM.png)", file=out)

print("\n- Initial Parameters:", par_init, file=out)
print("\n- Function Evaluations:", infodict['nfev'], file=out)
print("\n- Convergence Message", mesg, file=out)
print("\n- Return Status", ier, file=out)

plt.plot(dim, milkbot_(dim, *popt), 'g-',
         label='LM Fit: a=%5.3f, b=%5.3f, c=%5.3f, d=%5.3f' % tuple(popt))

print("## Constrained Optimization: Trust Region Reflective Algorithm", file=out)
popt, pcovbnd = curve_fit(milkbot_, df.DIM, df.ECM_RAW, p0=par_init, sigma=meas_error, bounds=bounds)
print("\n- Milkbot Parameter Estimates a=%5.3f, b=%5.3f, c=%5.3f, d=%5.3f" % tuple(popt), file=out)
print("\n- Error Correlation Matrix", file=out)
print("\n![Error Correlations LM](docs/ecm_error_correlations_BND.png)", file=out)
print("\n- Initial Parameters:    ", par_init, file=out)
print("\n- Parameter Lower Bounds:", bounds[0], file=out)
print("\n- Parameter Upper Bounds:", bounds[1], file=out)


plt.plot(dim, milkbot_(dim, *popt), 'b:',
         label='Bounded Fit: a=%5.3f, b=%5.3f, c=%5.3f, d=%5.3f' % tuple(popt))

plt.legend()
plt.grid()
plt.title("Estimating ECM Using Unbounded and Bounded Minimization")
plt.savefig('docs/ecm_estimation.png', bbox_inches='tight')
plt.close()

print("## Comparison", file=out)
print("\n![ECM v DIM](./docs/ecm_estimation.png)", file=out)


plt.figure(figsize=(4,3))
sns.heatmap(cov2corr(pcovlm), fmt='.1g', annot=True)
plt.savefig('docs/ecm_error_correlations_LM.png', bbox_inches='tight')
plt.close()


plt.figure(figsize=(4,3))
sns.heatmap(cov2corr(pcovbnd), fmt='.1g', annot=True)
plt.savefig('docs/ecm_error_correlations_BND.png', bbox_inches='tight')
plt.close()


out.close()
