#!/usr/bin/env python
# encoding: utf-8
# Time    : 1/23/2019 5:24 PM
# Author  : Luzaofa

import time
import multiprocessing as mp
from urllib.request import urlopen
import os
import pandas as pd


class MySpider(object):

    def __init__(self):

        self.base_url = 'http://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1815_stock&TABKEY=tab1&txtBeginDate={start}&txtEndDate={end}'

    def data_mp(self, func, mass):
        '''
            进程池
        '''
        pool = mp.Pool(processes=4)
        for i in mass:
            pool.apply_async(func, args=(i[0], i[1],))
        pool.close()
        pool.join()

    def download_excel(self, url, filename):
        '''
            保存网页excel文件到本地  'XX.xlsx'
        '''
        with urlopen(url) as response:
            with open(filename, 'wb') as f:
                f.write(response.read())

    def analysis_excel(self, filename):
        '''
            excel数据解析,获取指定列数据
        '''
        data = pd.read_excel(filename, sheet_name=0, header=0, dtype=object)
        result = data[['证券代码', '前收', '今收']]
        return result

    def save_csv(self, data, csvname):
        '''
            文件存储，保存csv文件
        '''
        data.to_csv(csvname, index=0)

    def logic(self, url, csvname):
        '''
            业务逻辑人口
        '''
        filename = 'log.xlsx'
        self.download_excel(url, filename)
        result = self.analysis_excel('log.xlsx')
        self.save_csv(result, csvname)

        try:
            os.path.exists(os.getcwd() + '/' + filename)
            os.remove(filename)
        except IOError:
            print('No such file')

    def main(self, url):
        '''
            程序启动主入口
        '''
        start = time.time()
        self.data_mp(self.logic, url)
        end = time.time()
        print('业务处理总耗时：%s 秒！' % (end - start))


if __name__ == '__main__':
    print('Start！')

    demo = MySpider()

    start = '2019-01-23'
    end = '2019-01-23'
    csvfile = 'mycsv.csv'

    url = [[demo.base_url.format(start=start, end=end), csvfile]]
    demo.main(url)

    print('END')
