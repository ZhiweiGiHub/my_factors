import pandas as pd
import datetime


def sw_info_(chooseday_r, sw_type):

    if len(chooseday_r) == 8:
        chooseday_r = chooseday_r[0:4] + '-' + chooseday_r[4:6] + '-' + chooseday_r[6:8]
    else:
        pass

    if sw_type == 'sw3':
        indu_info = pd.read_excel('C:/Users/Lenovo/PycharmProjects/my_project/frjj_codes/sw3_code_ts.xlsx')
        indu_name = pd.read_excel('C:/Users/Lenovo/PycharmProjects/my_project/frjj_codes/sw3_cn_list.xlsx')

        indu_info.rename(columns={'Unnamed: 0': 'date'}, inplace=True)
        indu_name.rename(columns={'sw_code': 'sw3_code'}, inplace=True)

        indu_info['date'] = [datetime.datetime.strftime(indu_info['date'].iloc[i],
                                                        '%Y-%m-%d') for i in range(len(indu_info))]
        indu_info = indu_info[indu_info['date'] == chooseday_r]
        sw3 = pd.DataFrame(columns=['date', 'code', 'sw3_code'], index=range(len(list(indu_info.columns)) - 1))
        sw3['date'] = indu_info.values[0][0]
        sw3['code'] = list(indu_info.columns)[1:]
        sw3['sw3_code'] = indu_info.values[0][1:]
        sw3 = sw3.merge(indu_name, on='sw3_code', how='left')
        sw3 = sw3.dropna(axis=0, how='any').reset_index(drop=True)
        return sw3
    elif sw_type == 'sw1':
        indu_info = pd.read_excel('C:/Users/Lenovo/PycharmProjects/my_project/frjj_codes/sw1_code_ts.xlsx')
        indu_name = pd.read_excel('C:/Users/Lenovo/PycharmProjects/my_project/frjj_codes/sw1_cn_list.xlsx')

        indu_info.rename(columns={'Unnamed: 0': 'date'}, inplace=True)
        indu_name.rename(columns={'sw_code': 'sw1_code'}, inplace=True)

        indu_info['date'] = [datetime.datetime.strftime(indu_info['date'].iloc[i],
                                                        '%Y-%m-%d') for i in range(len(indu_info))]
        indu_info = indu_info[indu_info['date'] == chooseday_r]
        sw1 = pd.DataFrame(columns=['date', 'code', 'sw1_code'], index=range(len(list(indu_info.columns)) - 1))
        sw1['date'] = indu_info.values[0][0]
        sw1['code'] = list(indu_info.columns)[1:]
        sw1['sw1_code'] = indu_info.values[0][1:]
        sw1 = sw1.merge(indu_name, on='sw1_code', how='left')
        sw1 = sw1.dropna(axis=0, how='any').reset_index(drop=True)
        return sw1


