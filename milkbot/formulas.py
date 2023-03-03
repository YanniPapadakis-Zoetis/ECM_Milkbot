import numpy as np

# Milkbot Formula
def milkbot(par: np.array, DIM: np.array):
    a, b, c, d = par
    return a * (1 - np.exp((c - DIM)/b)/2) * np.exp(-d*DIM)

# International ECM correction equation
# ECM milk = (milk production * (0.383 * % fat + 0.242 * % protein + 0.7832) / 3.1138)
ecm_milk = lambda milk, pctf, pctp: milk * (0.383 * pctf + 0.242 * pctp + 0.7832) / 3.1138
