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

def run_example():
    milk = np.array([45, 73,  81,  81,  83,  81,  80,  72,  79,  71,  69,  69,  65,  61])
    dim  = np.array([13, 41,  68, 107, 132, 167, 195, 226, 254, 283, 310, 346, 377, 405])
    return milkbot_solution(milk, dim)

def main():

    soln = run_example()

    print(soln.x)
    print(soln.fun)
    print(soln.status)
    print(soln.success)
    print(soln.nfev)
    
if __name__ == '__main__':
    main()