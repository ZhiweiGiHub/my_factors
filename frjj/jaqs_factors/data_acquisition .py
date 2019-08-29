from jaqs_fxdayu.data import DataView
from jaqs_fxdayu.data import RemoteDataService  # 远程数据服务类

data_config = {
    "remote.data.address": "tcp://data.quantos.org:8910",
    "remote.data.username": "17665139597",
    "remote.data.password": "eyJhbGciOiJIUzI1NiJ9.eyJjcmVhdGVfdGltZSI6IjE1NjM5NDc0MzYwMzEiLCJpc3MiOiJhdXRoMCIsImlkIjoiMTc2NjUxMzk1OTcifQ.crFpJTlrqcduHE7zEMms2XhAoggttMLlFSkGWAzyNhY"
}

# step 2
ds = RemoteDataService()
ds.init_from_config(data_config)
dv = DataView()

sub_period = [(20190130, 20190630)]

file_name = ['data']

universe = '000300.SH'

# step 3
for i in range(len(sub_period)):
    props = {'start_date': sub_period[i][0],
             'end_date': sub_period[i][1],
             'universe': universe,
             'fields': "pb,pe,oper_exp,sw1",
             'report_type': '408003000',
             'freq': 1}

    dv.init_from_config(props, ds)
    dv.prepare_data()
    dv.save_dataview('C:/Users/Lenovo/PycharmProjects/my_factor/' + file_name[i] + '_' + universe[0:6])
    print(sub_period[i], 'finished')
