import numpy as np
import pandas as pd

df = pd.DataFrame(np.random.randn(4, 3),
                  columns=list('bde'),
                  index=['utah', 'ohio', 'texas', 'oregon'])

f = lambda x: x.max() - x.min()

# 默认情况下会以列为单位，分别对列应用函数

t1 = df.apply(f)
t2 = df.apply(f, axis=1)

from joblib import Parallel, delayed
from math import sqrt

# %time
result1 = Parallel(n_jobs=1)(delayed(sqrt)(i ** 2) for i in range(10000))
# %time
result2 = Parallel(n_jobs=8)(delayed(sqrt)(i ** 2) for i in range(10000))

df1 = pd.DataFrame({'a': [1, 2, 1, 2, 1, 2],
                    'b': [3, 3, 3, 4, 4, 4],
                    'data': [12, 13, 11, 8, 10, 3]})

grouped = df1.groupby('b')

for name, group in grouped:
    print(name, '->')
    print(group)

grouped = df1.groupby(['a', 'b'])
# 按照 'b' 这列分组了，name 为 'b' 的 key 值，group 为对应的df_group
for name, group in grouped:
    print(name, '->')
    print(group)

grouped = df1.groupby(df1.index)
# 按照 index 分组，其实每行就是一个组了
for name, group in grouped:
    print(name, '->')
    print(group)


def tmp_func(df):
    df['result'] = df['data'].apply(sqrt)  # 注意这里apply里面使用的是函数名，不需要加括号
    return df


def apply_parallel(df_grouped, func):
    """利用 Parallel 和 delayed 函数实现并行运算"""
    results = Parallel(n_jobs=-1)(delayed(func)(group) for name, group in df_grouped)
    # 这里的n_jobs为并行调用的CPU核心，如果-1，则使用所有CPU。如果给定1，则没有并行计算代码
    return pd.concat(results)


df_grouped = df1.groupby(df1.index)
df_all = apply_parallel(df_grouped, tmp_func)

# 多for循环进行并行处理
import time
from multiprocessing.dummy import Pool as ThreadPool


def process(param):
    print(param['a'] + param['b'])
    time.sleep(5)
    return param['a']


items = list(range(9))
t = list(range(9))
pool = ThreadPool()
param = {'a': items, 'b': t}
results = pool.map(process, param)
pool.close()
pool.join()


# OOP20190805

class FactorInvestment(object):
    factor_list = []

    def __init__(self, name):
        self.name = name
        self.factor_list.append(name)


def product(*args):
    if len(args) != 0:
        products = 1
        for i in args:
            products = products * i
        return products
    else:
        raise TypeError


print('product(5) =', product(5))
print('product(5, 6) =', product(5, 6))
print('product(5, 6, 7) =', product(5, 6, 7))
print('product(5, 6, 7, 9) =', product(5, 6, 7, 9))
if product(5) != 5:
    print('测试失败!')
elif product(5, 6) != 30:
    print('测试失败!')
elif product(5, 6, 7) != 210:
    print('测试失败!')
elif product(5, 6, 7, 9) != 1890:
    print('测试失败!')
else:
    try:
        product()
        print('测试失败!')
    except TypeError:
        print('测试成功!')


def ji(**kargs):
    print(kargs)


class My_test:
    f = 9


a = My_test

try:
    raise ValueError("This is an argument")
except ValueError as e:
    print("The exception argments were", e.args)

g = (x for x in range(10))  # 注意，g这里为一个迭代器，而不是一个列表

for i in g:
    print(i)

from collections import Counter

responses = [
    "vanilla",
    "chocolate",
    "vanilla",
    "vanilla",
    "caramel",
    "strawberry",
    "vanilla"
]

Counter(responses).most_common()

# 格式化输出
print('hello {0}, how are you? {1}'.format('Mike', 'where are you?'))

# 关于groupby函数和抽象函数的结合
df = pd.DataFrame(columns=['name', 'price', 'size'], index=range(10))
df['name'] = ['A', 'A', 'B', 'B', 'B', 'C', 'C', 'C', 'C', 'D']
df['price'] = [100, 90, 100, 110, 120, 80, 70, 90, 60, 150]
df['size'] = ['M', 'S', 'M', 'M', 'L', 'S', 'S', 'M', 'S', 'L']

# 找出每个人手上房子price最大的房子的size。
cc = df.groupby('name').apply(lambda sub: sub['size'][sub['price'].idxmax()])
# apply所处理的数据为一个子集，或者一行数据

# 使用全局变量
series = []


def t():
    global series
    series.append(1)


for i in range(10):
    t()

from abc import ABCMeta, abstractmethod
from random import randint, randrange


class Fighter(object, metaclass=ABCMeta):
    """战斗者"""

    # 通过__slots__魔法限定对象可以绑定的成员变量
    __slots__ = ('_name', '_hp')

    def __init__(self, name, hp):
        """初始化方法

        :param name: 名字
        :param hp: 生命值
        """
        self._name = name
        self._hp = hp

    @property
    def name(self):
        return self._name

    @property
    def hp(self):
        return self._hp

    @hp.setter
    def hp(self, hp):
        self._hp = hp if hp >= 0 else 0

    @property
    def alive(self):
        return self._hp > 0

    @abstractmethod
    def attack(self, other):
        """攻击

        :param other: 被攻击的对象
        """
        pass


class Ultraman(Fighter):
    """奥特曼"""

    __slots__ = ('_name', '_hp', '_mp')

    def __init__(self, name, hp, mp):
        """初始化方法

        :param name: 名字
        :param hp: 生命值
        :param mp: 魔法值
        """
        super().__init__(name, hp)
        self._mp = mp

    def attack(self, other):
        other.hp -= randint(15, 25)

    def huge_attack(self, other):
        """究极必杀技(打掉对方至少50点或四分之三的血)

        :param other: 被攻击的对象

        :return: 使用成功返回True否则返回False
        """
        if self._mp >= 50:
            self._mp -= 50
            injury = other.hp * 3 // 4
            injury = injury if injury >= 50 else 50
            other.hp -= injury
            return True
        else:
            self.attack(other)
            return False

    def magic_attack(self, others):
        """魔法攻击

        :param others: 被攻击的群体

        :return: 使用魔法成功返回True否则返回False
        """
        if self._mp >= 20:
            self._mp -= 20
            for temp in others:
                if temp.alive:
                    temp.hp -= randint(10, 15)
            return True
        else:
            return False

    def resume(self):
        """恢复魔法值"""
        incr_point = randint(1, 10)
        self._mp += incr_point
        return incr_point

    def __str__(self):
        return '~~~%s奥特曼~~~\n' % self._name + \
               '生命值: %d\n' % self._hp + \
               '魔法值: %d\n' % self._mp


class Monster(Fighter):
    """小怪兽"""

    __slots__ = ('_name', '_hp')

    def attack(self, other):
        other.hp -= randint(10, 20)

    def __str__(self):
        return '~~~%s小怪兽~~~\n' % self._name + \
               '生命值: %d\n' % self._hp


def is_any_alive(monsters):
    """判断有没有小怪兽是活着的"""
    for monster in monsters:
        if monster.alive > 0:
            return True
    return False


def select_alive_one(monsters):
    """选中一只活着的小怪兽"""
    monsters_len = len(monsters)
    while True:
        index = randrange(monsters_len)
        monster = monsters[index]
        if monster.alive > 0:
            return monster


def display_info(ultraman, monsters):
    """显示奥特曼和小怪兽的信息"""
    print(ultraman)
    for monster in monsters:
        print(monster, end='')


def main():
    u = Ultraman('骆昊', 1000, 120)
    m1 = Monster('狄仁杰', 250)
    m2 = Monster('白元芳', 500)
    m3 = Monster('王大锤', 750)
    ms = [m1, m2, m3]
    fight_round = 1
    while u.alive and is_any_alive(ms):
        print('========第%02d回合========' % fight_round)
        m = select_alive_one(ms)  # 选中一只小怪兽
        skill = randint(1, 10)  # 通过随机数选择使用哪种技能
        if skill <= 6:  # 60%的概率使用普通攻击
            print('%s使用普通攻击打了%s.' % (u.name, m.name))
            u.attack(m)
            print('%s的魔法值恢复了%d点.' % (u.name, u.resume()))
        elif skill <= 9:  # 30%的概率使用魔法攻击(可能因魔法值不足而失败)
            if u.magic_attack(ms):
                print('%s使用了魔法攻击.' % u.name)
            else:
                print('%s使用魔法失败.' % u.name)
        else:  # 10%的概率使用究极必杀技(如果魔法值不足则使用普通攻击)
            if u.huge_attack(m):
                print('%s使用究极必杀技虐了%s.' % (u.name, m.name))
            else:
                print('%s使用普通攻击打了%s.' % (u.name, m.name))
                print('%s的魔法值恢复了%d点.' % (u.name, u.resume()))
        if m.alive > 0:  # 如果选中的小怪兽没有死就回击奥特曼
            print('%s回击了%s.' % (m.name, u.name))
            m.attack(u)
        display_info(u, ms)  # 每个回合结束后显示奥特曼和小怪兽的信息
        fight_round += 1
    print('\n========战斗结束!========\n')
    if u.alive > 0:
        print('%s奥特曼胜利!' % u.name)
    else:
        print('小怪兽胜利!')


if __name__ == '__main__':
    main()
