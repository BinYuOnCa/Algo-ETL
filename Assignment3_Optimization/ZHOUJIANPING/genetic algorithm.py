def demo_func(x):
    x1, x2, x3 = x
    return x1 ** 2 + (x2 - 0.05) ** 2 + x3 ** 2

from ga import GA
ga = GA(func=demo_func, lb=[-1, -10, -5], ub=[2, 10, 2], max_iter=500)
best_x, best_y = ga.fit()
import pandas as pd
import matplotlib.pyplot as plt
FitV_history = pd.DataFrame(ga.FitV_history)
fig, ax = plt.subplots(2, 1)
ax[0].plot(FitV_history.index, FitV_history.values, '.', color='red')
plt_max = FitV_history.max(axis=1)
ax[1].plot(plt_max.index, plt_max, label='max')
ax[1].plot(plt_max.index, plt_max.cummax())
plt.show()
