import pandas as pd
import numpy as np
import timeit
from milkbot.formulas import milkbot_
from scipy.optimize import curve_fit
from sklearn.metrics import mean_squared_error

setup_string = """
import pandas as pd;
import numpy as np;
from milkbot.formulas import milkbot_;
from scipy.optimize import curve_fit;
par_init = np.array([90.0, 25.0, 15.0, 0.001]);
df = pd.read_csv('docs/example1.csv');
"""

nrep = 10
times = timeit.repeat(
    "curve_fit(milkbot_, df.DIM, df.MILK, p0=par_init, full_output=True)", 
    setup=setup_string,
    number=nrep, 
    repeat=5
    )

df = pd.read_csv("docs/example1.csv")
par_init = np.array([90.0, 25.0, 15.0, 0.001])
popt, pcov, infodict, mesg, ier = curve_fit(milkbot_, df.DIM, df.MILK, p0=par_init, full_output=True)
milk_pred = milkbot_(df.DIM, *popt)
mse = mean_squared_error(df.MILK, milk_pred)

print("Estimation Using the MilkBot Formula")
print(pd.DataFrame([popt], columns=list('abcd'), index=['Example 1']))
print("Estimation Optimal MSE: {:.3f}".format(mse))
print("Convergence Message:",mesg)
print("Solution Status = {}\nNo of Evaluations = {}".format(ier,infodict['nfev']))
print("Evaluation Time = {:.1f} msec".format(np.mean(times)*1000/nrep))
