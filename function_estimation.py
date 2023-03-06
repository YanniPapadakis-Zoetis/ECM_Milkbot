import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit
import seaborn as sns
#from milkbot.formulas import milkbot

def milkbot(DIM: np.array, a, b, c, d):
    return a * (1 - np.exp((c - DIM)/b)/2) * np.exp(-d*DIM)

df = pd.read_csv("docs/example1.csv")

par_init = np.array([90.0, 25.0, 15.0, 0.002])
bounds = (
    [0., 0., -30., 0.], 
    [200., 100., 30., 0.01]
    )
dim = np.arange(400)
milk_pred = milkbot(dim, *list(par_init))

plt.plot(df.DIM, df.MILK, 'ro', label='data')


popt, pcov = curve_fit(milkbot, df.DIM, df.MILK, p0=par_init)
print(popt)
print(pcov)

plt.plot(df.DIM, milkbot(df.DIM, *popt), 'g-',
         label='fit: a=%5.3f, b=%5.3f, c=%5.3f, d=%5.3f' % tuple(popt))

popt, pcov = curve_fit(milkbot, df.DIM, df.MILK, p0=par_init, bounds=bounds)

plt.plot(df.DIM, milkbot(df.DIM, *popt), 'b:',
         label='fit: a=%5.3f, b=%5.3f, c=%5.3f, d=%5.3f' % tuple(popt))

plt.legend()
plt.grid()
plt.show()

# using the upper triangle matrix as mask 
#sns.heatmap(pcov, fmt='.1g', annot=True)
#plt.show()
