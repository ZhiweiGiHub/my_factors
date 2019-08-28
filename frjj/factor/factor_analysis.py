from tqdm import tqdm
from factor.factor_maker import *

user = "huangwei"
passwd = "huangwei"

# 获取所有的交易日
tradingdays = get_tradingdays(user, passwd, '20170301', "20190628")

# 设定选股日期、调仓日期
change_days = []
choose_days = []

for i in range(len(tradingdays) - 1):
    if tradingdays['tradingdays'].iloc[i][4:6] != tradingdays['tradingdays'].iloc[i + 1][4:6]:
        # choose_days.append(tradingdays['tradingdays'][i])
        change_days.append(tradingdays['tradingdays'][i + 1])

# 所有计算时点
all_date = pd.DataFrame(change_days, columns=['dates'])
all_date = all_date[all_date['dates'] < "20190628"]

# 将每个时点截面所估计的系数和系数的p值记录下来
df_result = pd.DataFrame(columns=['date', 'slope', 'pvalue_ols',
                                  'spearman', 'pvalue_cor'],
                         index=range(len(all_date)))
num = 0

for i in tqdm(range(len(all_date) - 1)):
    date = all_date['dates'].iloc[i]
    date_r = date[0:4] + '-' + date[4:6] + '-' + date[6:8]
    date2 = all_date['dates'].iloc[i + 1]
    date_r2 = date2[0:4] + '-' + date2[4:6] + '-' + date2[6:8]

    factor_value = base_indi_maker_(date_r,
                                    collection_name="d_tech_moneyflow20",
                                    indicator='TECH_MONEYFLOW20',
                                    ac=False, database="stock_cn_finance1", type=None)
    # 创建某时刻截面指标df
    df_values = pd.DataFrame(columns=['date', 'code', 'factor_value'],
                             index=range(len(factor_value)))
    df_values['date'] = date
    df_values['code'] = factor_value['code']
    df_values['factor_value'] = factor_value['TECH_MONEYFLOW20']  # 指标选择

    # 获取当下日期过去一个月的收益率数据
    info_all = get_data_panel_backwards("huangwei", "huangwei",
                                        list(df_values['code']),
                                        date_r,
                                        back_years=0, back_months=1,
                                        collection='stock_daily')

    info_all = info_all.rename(columns={'code': 'codes'})
    info_all['rtn'] = info_all['rtn'] + 1
    info_all = info_all[['codes', 'rtn', 'date']]

    # 分组统计
    grouped = info_all.groupby('codes').last()
    grouped_first = info_all.groupby(by=info_all['codes']).first()
    grouped_last = pd.DataFrame(columns=['rtn', 'date'], index=grouped_first.index)
    grouped_last['date'] = info_all['date'].iloc[-1]

    for index in grouped_last.index:
        temp = info_all[info_all['codes'] == index]
        grouped_last['rtn'].loc[index] = list(temp['rtn'].cumprod())[-1]

    momentums = grouped_last['rtn'] - grouped_first['rtn']

    df_price = pd.DataFrame(columns=['date', 'code', 'momentum'], index=range(len(grouped)))
    df_price['code'] = list(grouped.index)
    df_price['date'] = grouped['date'].iloc[0]
    df_price['momentum'] = list(momentums.values)
    df_price = df_price.sort_values(by='momentum', ascending=True).reset_index(drop=True)

    # 合并收盘价以及指标值数据，计算两者截面相关性
    df_all = df_price.merge(df_values[['code', 'factor_value']], how='inner', on='code')
    # 剔除不匹配的股票
    df_all = df_all.dropna(how='any').reset_index(drop=True)
    df_all = df_all.rename(columns={'momentum': 'return'})

    # IC及rankIC
    Y = df_all['return']
    X = df_all['factor_value']

    # 写入df_result
    df_result['date'].iloc[num] = date
    df_result['slope'].iloc[num] = OLS_param(Y, X)[0]
    df_result['pvalue_ols'].iloc[num] = OLS_param(Y, X)[1]
    df_result['spearman'].iloc[num] = spearman_cor(df_all['return'].values,
                                                   df_all['factor_value'].values)[0]
    df_result['pvalue_cor'].iloc[num] = spearman_cor(df_all['return'].values,
                                                     df_all['factor_value'].values)[1]
    num += 1

# 结果分析
df_result_analysis = pd.DataFrame(columns=['正相关显著比例', '负相关显著比例',
                                           '同向显著比例', '状态切换比例'], index=range(1))
num_date = len(df_result)
rate_1_count = 0
rate_2_count = 0
rate_3_count = 0
rate_4_count = 0
symbols = []  # 记录秩相关系数的符号

for i in range(num_date):
    if df_result['spearman'].iloc[i] >= 0 and df_result['pvalue_cor'].iloc[i] <= 0.05:  # 判断是否正向显著
        rate_1_count += 1
        symbols.append(1)
    elif df_result['spearman'].iloc[i] < 0 and df_result['pvalue_cor'].iloc[i] <= 0.05:
        rate_2_count += 1
        symbols.append(-1)

same_direction = 0
diff_direction = 0

for i in range(len(symbols)):
    if i == 0:
        last_symbols = symbols[i]
    else:
        last_symbol = symbols[i - 1]
        symbol = symbols[i]
        if symbol == last_symbol:
            same_direction += 1
        else:
            diff_direction += 1

df_result_analysis['正相关显著比例'] = rate_1_count / num_date
df_result_analysis['负相关显著比例'] = rate_2_count / num_date
df_result_analysis['同向显著比例'] = same_direction / num_date
df_result_analysis['状态切换比例'] = diff_direction / num_date
