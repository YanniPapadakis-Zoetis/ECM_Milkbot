from scipy.optimize import minimize, dual_annealing
from sklearn.metrics import mean_squared_error, mean_absolute_percentage_error, mean_squared_log_error, mean_absolute_error
import numpy as np
from milkbot.formulas import milkbot


def to_min_factory(y: np.array, x: np.array, fun):
    return lambda par: fun(y, milkbot(par,x))

def milkbot_solution(
    y: np.array,
    x: np.array,
    par_init: np.array = np.array([100.0, 25.0, -4.0, 0.0015]),
    bounds: list = [(0.0, None), (0.0, None), (None, 0.0), (0.0, None)],
    constraints: list = [ {'type': 'ineq','fun': lambda p:  0.1 - p[1] * p[3]}],
    verbose: bool = False
    ):
    soln = minimize(
        to_min_factory(y,x,mean_squared_error), 
        x0=par_init, 
        method='SLSQP', 
        bounds=bounds,
        constraints=constraints,
        tol=1e-12,
        options={'maxiter': 1e6, 'disp': verbose, 'ftol': 1e-8}
        )
    
    return soln


def milkbot_solution_b(
    y: np.array,
    x: np.array,
    bounds: np.array,
    x0: np.array,
    **kwargs
    ):
    soln = dual_annealing(
        to_min_factory(y,x,mean_squared_error), 
        bounds=bounds,
        maxiter=10000,
        no_local_search=True,
        x0=x0
        )
    
    return soln
