# 为即将导入数据库的数据添加一列主键（在末尾一列）


def add_key_(df):
    if 'suspend_date' in df.columns:
        date_indicator = 'suspend_date'
    elif 'trade_date' in df.columns:
        date_indicator = 'trade_date'
    elif 'f_ann_date' in df.columns and 'ann_date' in df.columns:
        date_indicator = 'f_ann_date'
    elif 'f_ann_date' not in df.columns and 'ann_date' in df.columns:
        date_indicator = 'ann_date'

    if 'index_code' in df.columns:
        code_name1 = 'index_code'
        code_name2 = 'con_code'
        df['key'] = df.apply(lambda x:
                             '1' + x[code_name1][0:6]
                             + x[code_name2][0:6] +
                             x[date_indicator], axis=1)
    else:
        code_name = 'ts_code'
        df['key'] = df.apply(lambda x:
                             '1' + x[code_name][0:6] +
                             x[date_indicator], axis=1)
    return df
