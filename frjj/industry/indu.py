import pandas as pd
from tqdm import tqdm
from api import get_data_crosssection_genaral, get_data_panel, get_tradingdays, get_data_crosssection
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.ticker as ticker

banchmark = '000905.SH'
index_for_simple_weight = '000905.SH'
top_num = None
begin_date = '20160104'  # 起始日期不用变动
end_date = "20190628"
industry_weight = '000905.SH'  # 或者'market'

# 读取申万三级行业
all = pd.read_excel('C:/Users/Lenovo/PycharmProjects/my_project/frjj_codes/A股申万行业_2016-12-31.xlsx')

# 计算行业内部流通市值比例
tot_industry = all.groupby('sw3_code').sum()
del tot_industry['sw1_code']
tot_industry['sw3_code'] = tot_industry.index
tot_industry = tot_industry.reset_index(drop=True)
all = all.merge(tot_industry, on='sw3_code', how='inner')
all['weight_in_industry'] = all['cir_mv_x'] / all['cir_mv_y']
all.rename(columns={'cir_mv_x': 'cir_mv', 'cir_mv_y': 'industry_total'},
           inplace=True)

# 获得申万一级每个行业的比重(全市场)
if industry_weight == 'market':
    tot_industry_sw1 = all.groupby('sw1_name').sum()
    tot_industry_sw1['sw1_name'] = tot_industry_sw1.index
    total_mv = tot_industry_sw1['cir_mv'].sum()
    tot_industry_sw1['industryweight_in_all'] = tot_industry_sw1['cir_mv'] / total_mv
    tot_industry_sw1.index.name = 'sw1_names'  # 更改level_name
    all = all.merge(tot_industry_sw1[['industryweight_in_all', 'sw1_name']], on='sw1_name', how='inner')
else:
    # 获得申万一级每个行业的比重（中证500）
    zz500_weight = get_data_crosssection_genaral("huangwei", "huangwei",
                                                 "index_weight", 'index_code', industry_weight)
    zz500_weight = zz500_weight[zz500_weight['date'] == '2019-07-02'].reset_index(drop=True)  # 最新日期
    zz500_weight.rename(columns={'stock_code': 'code'}, inplace=True)
    # 获取中证500中成分股的申万一级行业信息及流通市值信息
    zz500_weight = zz500_weight.merge(all[['code', 'sw1_name', 'cir_mv']])
    tot_industry_sw1 = zz500_weight.groupby('sw1_name').sum()
    tot_industry_sw1['sw1_name'] = tot_industry_sw1.index
    total_mv = tot_industry_sw1['cir_mv'].sum()
    tot_industry_sw1['industryweight_in_all'] = tot_industry_sw1['cir_mv'] / total_mv
    tot_industry_sw1.index.name = 'sw1_names'  # 更改level_name
    all = all.merge(tot_industry_sw1[['industryweight_in_all', 'sw1_name']], on='sw1_name', how='inner')

# 添加行业总市值排名
tot_industry = tot_industry.sort_values(by='cir_mv', ascending=False).reset_index(drop=True)
tot_industry['rank'] = tot_industry.index + 1

all = all.merge(tot_industry[['sw3_code', 'rank']],
                on='sw3_code',
                how='inner').sort_values(by='rank').reset_index(drop=True)

# 追加行业数量字段
count_industry = all.groupby('sw3_code').count()
count_industry['indu_num'] = count_industry['code']
count_industry['sw3_code'] = count_industry.index
count_industry.index.name = 'sw3_codes'
all = all.merge(count_industry[['sw3_code', 'indu_num']],
                on='sw3_code',
                how='inner')

# 每个行业先保留前5个
delete_list = []
indu_list = list(all['sw3_name'].unique())

for indu in tqdm(indu_list):
    one_indu = all[all['sw3_name'] == indu]
    one_indu['global_index'] = one_indu.index
    if one_indu['indu_num'].iloc[0] > 5:
        one_indu = one_indu.sort_values(by='weight_in_industry',
                                        ascending=False).reset_index(drop=True)
        delete_list = delete_list + list(one_indu['global_index'].iloc[5:])

top5_industry = all.drop(delete_list).reset_index(drop=True)
top5_industry['ten_percent'] = round(top5_industry['indu_num'] * 0.1)

delete_list = []
for indu in tqdm(indu_list):
    one_indu = top5_industry[top5_industry['sw3_name'] == indu]
    one_indu['global_index'] = one_indu.index
    if one_indu['ten_percent'].iloc[0] < 5:
        one_indu = one_indu.sort_values(by='weight_in_industry',
                                        ascending=False)
        one_indu_withoutbig = one_indu[one_indu['weight_in_industry'] < 0.2].reset_index(drop=True)
        if len(one_indu_withoutbig) > 0:
            remain_num = one_indu_withoutbig['ten_percent'].iloc[0] - (len(one_indu) - len(one_indu_withoutbig))
            if remain_num <= 0:
                delete_list = delete_list + list(one_indu_withoutbig['global_index'])
            else:
                delete_list = delete_list + list(one_indu_withoutbig['global_index'].iloc[int(remain_num):])

result = top5_industry.drop(delete_list).reset_index(drop=True)

if top_num == None:
    result_top100mv = result
else:
    result_top100mv = result[result['rank'] <= top_num]

# 按成分股权重对龙头加权
hs300_weight = get_data_crosssection_genaral("huangwei", "huangwei",
                                             "index_weight", 'index_code', index_for_simple_weight)
hs300_weight = hs300_weight[hs300_weight['date'] == '2019-07-02'].reset_index(drop=True)
hs300_weight.rename(columns={'stock_code': 'code'},
                    inplace=True)

# 与沪深300股票取交集
result_top100mv = result_top100mv.merge(hs300_weight[['code', 'i_weight']],
                                        on='code',
                                        how='inner')
weight_sum = result_top100mv['i_weight'].sum()
result_top100mv['weight_adj'] = result_top100mv['i_weight'] / weight_sum

# 不与指数取交集，按照申万一级行业划分计算权重
tot_industry_sw1 = result.groupby('sw1_name').sum()
del tot_industry_sw1['sw1_code']
tot_industry_sw1['sw1_name'] = tot_industry_sw1.index
tot_industry_sw1 = tot_industry_sw1.reset_index(drop=True)
result = result.merge(tot_industry_sw1[['sw1_name', 'cir_mv']],
                      on='sw1_name',
                      how='inner')
result['weight_in_industry_sw1'] = result['cir_mv_x'] / result['cir_mv_y']
result.rename(columns={'cir_mv_x': 'cir_mv', 'cir_mv_y': 'industry_total_sw1'}, inplace=True)

# 申万一级行业的比重*行业内部个股占整个行业的比重
result['weight_adj_sw1'] = result['industryweight_in_all'] * result['weight_in_industry_sw1']
result['weight_adj_sw1'] = result['weight_adj_sw1'] / result['weight_adj_sw1'].sum()

# 计算股票池的净值曲线
# 获得每只股票在回测期的日收益率（除权调整），累积日收益率数据
begin_date = '20160104'
end_date = "20190628"

data = get_data_panel("huangwei", "huangwei", list(result_top100mv['code']), begin_date, end_date)
data_unlimited = get_data_panel("huangwei", "huangwei", list(result['code']), begin_date, end_date)

dates = get_tradingdays("huangwei", "huangwei", begin_date, end_date)
dates = dates.rename(columns={'tradingdays': 'date'})
for i in range(len(dates)):
    dates['date'].iloc[i] = dates['date'].iloc[i][0:4] + '-' + \
                            dates['date'].iloc[i][4:6] + '-' + dates['date'].iloc[i][6:8]
df_group_data_limited = dates
df_group_data_unlimited = dates

# 限定在沪深300成分股内
for code in list(result_top100mv['code']):
    temp = data[data['code'] == code].reset_index(drop=True)
    unique_index = list(temp['date'].drop_duplicates(keep='first',
                                                     inplace=False).index)
    temp = temp.iloc[unique_index]
    df_group_data_limited = df_group_data_limited.merge(temp[['date', 'rtn']],
                                                        how='outer',
                                                        on='date')
    df_group_data_limited = df_group_data_limited.rename(columns={'rtn': code})

df_group_data_limited = df_group_data_limited[df_group_data_limited['date'] >= '2017-01-01'].reset_index(drop=True)

# 缺失的收益率用0来代替
df_group_data_limited = df_group_data_limited.fillna(0)

# 不限定在成分股内
for code in list(result['code']):
    temp = data_unlimited[data_unlimited['code'] == code].reset_index(drop=True)
    unique_index = list(temp['date'].drop_duplicates(keep='first',
                                                     inplace=False).index)
    temp = temp.iloc[unique_index]
    df_group_data_unlimited = df_group_data_unlimited.merge(temp[['date', 'rtn']],
                                                            how='outer',
                                                            on='date')
    df_group_data_unlimited = df_group_data_unlimited.rename(columns={'rtn': code})

df_group_data_unlimited = df_group_data_unlimited[df_group_data_unlimited['date'] >= '2017-01-01'].reset_index(
    drop=True)

# 缺失的收益率用0来代替
df_group_data_unlimited = df_group_data_unlimited.fillna(0)

# 权重矩阵
weight_sw3 = result_top100mv['weight_adj'].values
weight_sw1 = result['weight_adj_sw1'].values

# 加权求和
group_return_arr_sw1 = df_group_data_unlimited.values[:, 1:]
group_return_arr_sw3 = df_group_data_limited.values[:, 1:]

weighted_return_sw3 = [sum(group_return_arr_sw3[i, :] * weight_sw3) for i in
                       range(len(df_group_data_limited))]
weighted_return_sw1 = [sum(group_return_arr_sw1[i, :] * weight_sw1) for i in
                       range(len(df_group_data_unlimited))]

group_returns_sw3 = pd.DataFrame(columns=['date'],
                                 index=range(len(df_group_data_unlimited)))
group_returns_sw3['date'] = df_group_data_limited['date']
group_returns_sw3['rtn'] = weighted_return_sw3
group_returns_sw3['rtn'] = group_returns_sw3['rtn'] + 1
group_returns_sw3['rtn'] = group_returns_sw3['rtn'].cumprod()

group_returns_sw1 = pd.DataFrame(columns=['date'],
                                 index=range(len(df_group_data_unlimited)))
group_returns_sw1['date'] = df_group_data_unlimited['date']
group_returns_sw1['rtn'] = weighted_return_sw1
group_returns_sw1['rtn'] = group_returns_sw1['rtn'] + 1
group_returns_sw1['rtn'] = group_returns_sw1['rtn'].cumprod()

# 净值曲线及超额收益（对比指数）
hs300 = get_data_panel("huangwei", "huangwei",
                       [banchmark],  # 对标指数
                       group_returns_sw1['date'].iloc[0],
                       group_returns_sw1['date'].iloc[-1],
                       collection='index_rtn')
hs300['cum_chg'] = hs300['pct_chg'] / 100 + 1
hs300['cum_chg'] = hs300['cum_chg'].cumprod().values

extra_sw1 = group_returns_sw1['rtn'].values[:, np.newaxis] - hs300['cum_chg'].values[:, np.newaxis]
extra_sw3 = group_returns_sw3['rtn'].values[:, np.newaxis] - hs300['cum_chg'].values[:, np.newaxis]

plt.subplot(2, 1, 1)
plt.plot(group_returns_sw1.values[:, 0],
         group_returns_sw1.values[:, 1],
         label='strategy_sw1')
plt.plot(group_returns_sw3.values[:, 0],
         group_returns_sw3.values[:, 1],
         label='strategy_sw3')
plt.plot(group_returns_sw1.values[:, 0],
         hs300['cum_chg'].values[:, np.newaxis],
         label=banchmark)
plt.gca().xaxis.set_major_locator(ticker.MultipleLocator(int(len(group_returns_sw1) / 6)))
plt.grid(True)     # 增加格点
plt.axis('tight')  # 坐标轴适应数据量 axis 设置坐标轴
plt.legend()
plt.xlabel('Date')

plt.subplot(2, 1, 2)
plt.plot(group_returns_sw1.values[:, 0],
         extra_sw1,
         label='extra_return_sw1')
plt.plot(group_returns_sw3.values[:, 0],
         extra_sw3,
         label='extra_return_sw3')

plt.gca().xaxis.set_major_locator(ticker.MultipleLocator(int(len(group_returns_sw1) / 6)))
plt.grid(True)     # 增加格点
plt.axis('tight')  # 坐标轴适应数据量 axis 设置坐标轴
plt.legend()
plt.xlabel('Date')

# 输出持仓名单
# result_top100mv.to_csv('result_top100mv.csv', encoding='utf-8')
