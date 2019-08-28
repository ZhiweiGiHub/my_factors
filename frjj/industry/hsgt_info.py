import pandas as pd
import datetime
import math

def hsgt_info_(chooseday_r):
    if len(chooseday_r) == 8:
        chooseday_r = chooseday_r[0:4] + '-' \
                      + chooseday_r[4:6] + '-' \
                      + chooseday_r[6:8]
    if chooseday_r == '2018-03-30':
        chooseday_r = '2018-02-28'
    hs_info = pd.read_excel('C:/Users/Lenovo/PycharmProjects/my_project/frjj_codes/沪深港通持股.xlsx')

    hs_info['date'] = [datetime.datetime.strftime(hs_info['date'].iloc[i],
                                                    '%Y-%m-%d') for i in range(len(hs_info))]
    hs_info = hs_info[hs_info['date'] == chooseday_r]
    hs_info_df = pd.DataFrame(columns=['date', 'code', '占自由流通股比(%)'],
                              index=range(len(list(hs_info.columns)) - 1))
    hs_info_df['date'] = hs_info.values[0][0]
    hs_info_df['code'] = list(hs_info.columns)[1:]
    hs_info_df['占自由流通股比(%)'] = hs_info.values[0][1:]
    hs_info_df = hs_info_df.dropna(axis=0, how='any').reset_index(drop=True)
    hs_info_df.rename(columns={'占自由流通股比(%)': 'hgt_ratio'}, inplace=True)
    hs_info_df['hgt_ratio'] = (hs_info_df['hgt_ratio'] + 1).apply(lambda x: math.log(x))
    return hs_info_df

if __name__ == "__main__":
    chooseday_r = "2019-06-28"
    ss = hsgt_info_(chooseday_r)