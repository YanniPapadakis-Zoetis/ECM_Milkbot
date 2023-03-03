import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.linear_model import LinearRegression
from milkbot.mse_min import milkbot_solution
from milkbot.formulas import milkbot, ecm_milk

df = pd.read_csv("docs/example1.csv")

# Plot Pct Fat / Protein v DIM
X = df['DIM'].values.reshape(-1,1)
regf = LinearRegression().fit(X[1:], df['PCTF'].iloc[1:].values)
regp = LinearRegression().fit(X[1:], df['PCTP'].iloc[1:].values)
df['PCTF_EST'] = regf.predict(X)
df['PCTP_EST'] = regp.predict(X)
plt.plot(df['DIM'],df['PCTF'],'ro',label="PCTF")
plt.plot(df['DIM'],df['PCTF_EST'],'r--',label="PCTF_EST")
plt.plot(df['DIM'],df['PCTP'],'go',label="PCTP")
plt.plot(df['DIM'],df['PCTP_EST'],'g--',label="PCTP_EST")
plt.legend()
plt.grid()
plt.savefig('docs/pctf_pctp_by_dim.png', bbox_inches='tight')
plt.close()

# Plot MILK v DIM
dim = np.arange(df['DIM'].max())
soln = milkbot_solution(df['MILK'], df['DIM'])
milk_est = milkbot(soln.x, dim)
plt.plot(df['DIM'],df['MILK'],'ro')
plt.plot(dim,milk_est,'b-')
plt.grid()
plt.savefig('docs/milk_by_dim.png', bbox_inches='tight')
plt.close()

# Plot ECM v DIM
df['ECM_RAW'] = df.apply(lambda x: ecm_milk(x.MILK, x.PCTF, x.PCTP), axis=1)
ecm_raw_soln = milkbot_solution(df['ECM_RAW'], df['DIM'])
ecm_raw_est = milkbot(ecm_raw_soln.x, dim)
df['ECM'] = df.apply(lambda x: ecm_milk(x.MILK, x.PCTF_EST, x.PCTP_EST), axis=1)
ecm_soln = milkbot_solution(df['ECM'], df['DIM'])
ecm_est = milkbot(ecm_soln.x, dim)
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(8, 4), sharey=True)
ax1.plot(df['DIM'],df['ECM_RAW'],'ko',label='ECM_RAW')
ax2.plot(df['DIM'],df['ECM'],'bo',label='ECM')
ax1.plot(dim,ecm_raw_est,'k:',label='ECM_RAW_EST')
ax2.plot(dim,ecm_est,'b:',label='ECM_EST')
ax1.legend()
ax2.legend()
ax1.grid()
ax2.grid()
fig.suptitle('ECM Estimation: Raw v Using Estimated % Fat and % Protein')
plt.savefig('docs/ecm_raw_by_dim.png', bbox_inches='tight')
plt.close()

