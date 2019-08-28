# 对已知多因子矩阵进行对称正交化
import numpy as np


def lowdin_orthogonal(data):
    data_ = data.copy()
    col = [i for i in data_.columns.tolist() if i not in ['code', 'date', 'weight_adj_sw1']]
    F = np.array(data_[col])  # 除去第一列行业指标,将数据框转化为矩阵
    M = np.dot(F.T, F)
    a, U = np.linalg.eig(M)  # U为特征向量，a为特征值
    one = np.identity(len(col))
    D = one * a  # 生成有特征值组成的对角矩阵
    D_inv = np.linalg.inv(D)
    S = U.dot(np.sqrt(D_inv)).dot(U.T)
    data_[col] = data_[col].dot(S)
    return data_