import pandas as pd
import numpy as np
import timeit
from milkbot.mse_min import milkbot_solution

nrep = 10
times = timeit.repeat(
    "milkbot_solution(df['MILK'], df['DIM'])", 
    setup="from milkbot.mse_min import milkbot_solution; import pandas; df = pandas.read_csv('docs/example1.csv')",
    number=nrep, 
    repeat=5
    )

df = pd.read_csv("docs/example1.csv")
soln = milkbot_solution(df['MILK'], df['DIM'])

print("Estimation Using the MilkBot Formula")
print(pd.DataFrame([soln.x], columns=list('abcd'), index=['Example 1']))
print("Estimation Optimal MSE: {:.3f}".format(soln.fun))
print("Solution Status = {} / Convergence = {} / No of Evaluations = {}".format(
    soln.status,soln.success,soln.nfev))
print("Evaluation Time = {:.1f} msec".format(np.mean(times)*1000/nrep))
