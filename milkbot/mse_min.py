from scipy.optimize import minimize
from sklearn.metrics import mean_squared_error
import numpy as np

def milkbot(par: np.array, DIM: np.array):
    a, b, c, d = par
    return a * (1 - np.exp((c - DIM)/b)/2) * np.exp(-d*DIM)

def to_min_factory(y: np.array, x: np.array):
    return lambda par: mean_squared_error(y, milkbot(par,x))

def milkbot_solution(
    y: np.array,
    x: np.array,
    par_init: np.array = np.array([100.0, 25.0, -4.0, 0.0015]),
    verbose: bool = False
    ):
    soln = minimize(
        to_min_factory(y,x), 
        x0=par_init, 
        method='SLSQP', 
        bounds=[(0.0, None), (0.0, None), (None, 0.0), (0.0, None)],
        constraints=[
            {'type': 'ineq','fun': lambda p:  0.1 - p[1] * p[3]}
            ],
        options={'maxiter': 1e6, 'disp': verbose, 'ftol': 1e-4}
        )
    
    return soln
