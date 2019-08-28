import pandas as pd
import matplotlib.pyplot as plt

holdings = pd.read_excel('C:/Users/Lenovo/PycharmProjects/my_project/frjj_codes/持仓股比较.xls',
                         header=None)

change_rate = []

for i in range(len(holdings.columns) - 1):
    temp = holdings[i].dropna(axis=0, how='any')
    temp_c = holdings[i + 1].dropna(axis=0, how='any')

    sell = len([stock for stock in list(temp) if stock not in list(temp_c)])
    new = len([stock for stock in list(temp_c) if stock not in list(temp)])

    change_rate.append(2 * (sell) / (len(temp) + len(temp_c)))

plt.plot(change_rate)
