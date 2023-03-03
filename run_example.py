import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from milkbot.mse_min import milkbot_solution, milkbot

df = pd.read_csv("docs/example1.csv")
print(df)

soln = milkbot_solution(df['MILK'], df['DIM'])

print(soln.x)
print(soln.fun)
print(soln.status)
print(soln.success)
print(soln.nfev)

dim = np.arange(df['DIM'].max())
milk_est = milkbot(soln.x, dim)

plt.plot(df['DIM'],df['PCTF'], 'ro', label="PCTF")
plt.plot(df['DIM'],df['PCTP'], 'go', label="PCTP")
plt.legend()
plt.grid()
plt.show()

plt.plot(df['DIM'],df['MILK'], 'ro')
plt.plot(dim,milk_est, 'b-')
plt.grid()
plt.show()
