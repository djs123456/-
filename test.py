# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import operator
from matplotlib import pyplot as plt
from matplotlib import font_manager
import requests
from lxml import etree
from pymongo import MongoClient
import re
from multiprocessing import Pool
from retrying import retry
import pymongo
from bson import ObjectId
import time as wtftime
time_str=wtftime.strftime("%Y%m%d%H")
name=time_str
my_font = font_manager.FontProperties(fname="C:\Windows\Fonts\simhei.ttf")  # 设置中文
uri = "mongodb://admin:woaljs456765!!@120.77.39.227:27017/"
mongo_client = pymongo.MongoClient(uri)
mongo_db = mongo_client['links']



class Linkspider():
    def __init__(self):
        self.base_url = "https://{city}.lianjia.com/ershoufang/pg{page}/"
        self.start_url = "https://{city}.lianjia.com/ershoufang/rs/"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'}

    def city(self):  # 获取用户输入
        try:
            city = input("请输入城市名:")
            return city
        except:
            print("没有这个城市的房价信息")
    @retry(stop_max_attempt_number=3)
    def url2html(self, url):
        try:
            response = requests.get(url, headers=self.headers)
            html = etree.HTML(response.text)
            return html
        except UnicodeDecodeError as e:
            print(e)

    def get_totalnum(self, city):  # 获取套房的所有数量
        start_url = self.start_url.format(city=city)
        html = self.url2html(start_url)
        totalnum = int(html.xpath('//h2[@class="total fl"]/span/text()')[0])
        print("共找到{totalnum}套{city}二手房".format(totalnum=totalnum, city=city))
        print("{city}区域的住房数如下(请稍后...)".format(city=city))
        print("*" * 100)

    def get_region(self, city):  # 获得该城市的区域列表
        start_url = self.start_url.format(city=city)
        html = self.url2html(start_url)
        regions = html.xpath('//div[@data-role="ershoufang"]/div/a/text()')  # 获得地区
        region_urls = html.xpath('//div[@data-role="ershoufang"]/div/a/@href')  # 获得地区url
        return region_urls, regions  # 两个返回的时候返回的是元组

    def get_region_url(self, city, region_urls, regions):  # func1为第一个功能,是否显示各个地区的二手房数量图,func1为1则显示数量图,如果不是,则继续往下执行
        numlist = []
        Bregion_url = []
        region_infos = []

        for region_url in region_urls:
            region_url = "https://{city}.lianjia.com/".format(city=city) + region_url
            Bregion_url.append(region_url)
            html = self.url2html(region_url)
            num = int(html.xpath('//h2[@class="total fl"]/span/text()')[0])
            numlist.append(num)
        for region_info in zip(regions, numlist):
            region_infos.append(region_info)
            print("{region}共有{num}套二手房".format(region=region_info[0], num=region_info[1]))
            print("*" * 100)
        try:
            self.func1(regions, numlist)
            print("开始获取各个区域的房价信息")
            print("你想获取哪个区域的信息?")
            print(regions)
            item = dict(zip(regions, region_urls))
            key = input("请输入:")
            while 1:
                if key in regions:
                    url = item[key]
                    areaurl = "https://{city}.lianjia.com/".format(city=city) + url  # 这里取得https://dg.lianjia.com//ershoufang/dapengxinqu/用户匹配的url可以用来请求,以获取信息
                    # 这里使用一个函数把areaurl传入,输出nanchengqu的房子信息
                    print("{key}附近的区域有:".format(key=key))
                    item = self.func2(areaurl, city)
                    self.func3(item)
                    print("进入详细分析模式")
                    print("开始爬取{city}的二手房信息(请稍后...)".format(city=city))

                    break
                else:
                    key = input("输入错误,请重新输入:")
                break
        except:
            print("输入错误")

        return Bregion_url


    def func1(self, regions, numlist):  # 利用matplotlib绘制区域图 两个对应列表转图
        df = pd.DataFrame(np.array(numlist), index=regions)
        df = df.sort_values(by=0, ascending=False)
        regions = list(df.index)
        numlist = sorted(numlist, reverse=True)
        plt.figure(figsize=(20, 8), dpi=80)
        x = range(len(regions))
        plt.bar(x, numlist, color="orange")
        plt.xticks(x, regions, fontproperties=my_font)
        plt.title('各区域的二手房数量条形图', fontproperties=my_font)
        plt.show()

    def func2(self, areaurl, city):  #
        area_urllist = []
        num_list = []
        area_html = self.url2html(areaurl)
        area = area_html.xpath('//div[@data-role="ershoufang"]/div[2]/a/text()')  # 这里显示的是小小分区,南城里面的分区
        area_hrefs = area_html.xpath('//div[@data-role="ershoufang"]/div[2]/a/@href')
        for area_href in area_hrefs:
            area_urllist.append("https://{city}.lianjia.com".format(city=city) + area_href)
        for url in area_urllist:
            html = self.url2html(url)
            num = int(html.xpath('//h2[@class="total fl"]/span/text()')[0])
            num_list.append(num)
        item = dict(zip(area, num_list))
        print(area)
        print("正在绘制条形图...")
        return item

    def func3(self, item):  # 输入字典字典转换成条形图,且需要排序
        df = sorted(item.items(), key=operator.itemgetter(1), reverse=True)  # 排序
        numlist = []
        regions = []
        for i in df:
            regions.append(i[0])
        for i in df:
            numlist.append(i[1])
        plt.figure(figsize=(20, 8), dpi=80)
        x = range(len(regions))
        plt.bar(x, numlist, color="blue")
        plt.xticks(x, regions, fontproperties=my_font)
        plt.title('各区域的二手房数量条形图', fontproperties=my_font)
        plt.show()
    def prompt(self):
        print("输入1:查看平均单价")
        print("输入2:查看平均总价")
        print("输入3:查看单价最高最低的10个房价信息")
        print("输入4:查看总价最高最低的10个房价信息")
        print("输入5:查看单价分布直方图")
        print("输入6:查看总价分布直方图")
        print("输入8:查看房价单价散点图")
        print("输入9:查看提示")
        print("输入0:退出,结束")

    def analyse(self):#提示
        data=self.get_data()
        while 1:
            try:
                num=int(input("请输入:"))
                if num==0:
                    print("正在退出...")
                    break
                elif num==1:
                    self.unit_analyse(data)
                elif num==2:
                    self.total_analyse(data)
                elif num==3:
                    self.unit_head_tail(data)
                elif num==4:
                    self.total_head_tail(data)
                elif num==5:
                    self.unit_pic(data)
                elif num==9:
                    self.prompt()
            except:
                print("请输入数字.")
                print("*"*100)

    def get_data(self):#获得data数据
        data=pd.read_csv('./csvfile/houseprice.csv')
        return data

    def unit_analyse(self,data):
        unit_mean=data["unit_Price"].mean()
        print(round(unit_mean,2),"元/平米")

    def total_analyse(self,data):
        total_mean = data["total_price"].mean()
        print(round(total_mean, 3), "万元")

    def unit_head_tail(self,data):
        print("单价最高的10个房价依次为:\n")
        df = data.sort_values(by="unit_Price", ascending=False).head(10)
        df.index = range(1, 11)
        df_title=list(df["title"])
        df_unit_price=list(df["unit_Price"])
        df_total_price=list(df["total_price"])
        head_tail_list=list(range(1,11))

        for item in zip(head_tail_list,df_title,df_unit_price,df_total_price):
            print(item[0],item[1])
            print("单价:"+str(item[2])+"元/平方米","\t","总价:"+str(item[3])+"万元\n")
        print("*" * 100)
        print("*"*100+"\n")
        print("单价最低的10个房价依次为:\n")
        df = data.sort_values(by="unit_Price").head(10)
        df.index = range(1, 11)
        df_title = list(df["title"])
        df_unit_price = list(df["unit_Price"])
        df_total_price = list(df["total_price"])
        head_tail_list = list(range(1, 11))
        for item in zip(head_tail_list,df_title,df_unit_price,df_total_price):
            print(item[0],item[1])
            print("单价:"+str(item[2])+"元/平方米","\t","总价:"+str(item[3])+"万元\n")
    def total_head_tail(self,data):
        print("总价最高的10个房价依次为:\n")
        df = data.sort_values(by="total_price", ascending=False).head(10)
        df.index = range(1, 11)
        df_title = list(df["title"])
        df_unit_price = list(df["unit_Price"])
        df_total_price = list(df["total_price"])
        head_tail_list = list(range(1, 11))

        for item in zip(head_tail_list, df_title, df_unit_price, df_total_price):
            print(item[0], item[1])
            print("单价:" + str(item[2]) + "元/平方米", "\t", "总价:" + str(item[3]) + "万元\n")
        print("*" * 100)
        print("*" * 100 + "\n")
        print("总价最低的10个房价依次为:\n")
        df = data.sort_values(by="total_price").head(10)
        df.index = range(1, 11)
        df_title = list(df["title"])
        df_unit_price = list(df["unit_Price"])
        df_total_price = list(df["total_price"])
        head_tail_list = list(range(1, 11))
        for item in zip(head_tail_list, df_title, df_unit_price, df_total_price):
            print(item[0], item[1])
            print("单价:" + str(item[2]) + "元/平方米", "\t", "总价:" + str(item[3]) + "万元\n")

    def unit_pic(self,data):
        df_2 = data[data["unit_Price"] < 20000]
        df_2_3 = data[(data["unit_Price"] < 30000) & (data["unit_Price"] > 20000)]
        df_3_4 = data[(data["unit_Price"] < 40000) & (data["unit_Price"] > 30000)]
        df_4_5 = data[(data["unit_Price"] < 50000) & (data["unit_Price"] > 40000)]
        df_5_6 = data[(data["unit_Price"] < 60000) & (data["unit_Price"] > 50000)]
        df_6_7 = data[(data["unit_Price"] < 70000) & (data["unit_Price"] > 60000)]
        df_7_8 = data[(data["unit_Price"] < 80000) & (data["unit_Price"] > 70000)]
        df_8_9 = data[(data["unit_Price"] < 90000) & (data["unit_Price"] > 80000)]
        df_9_10 = data[(data["unit_Price"] < 100000) & (data["unit_Price"] > 90000)]
        df_10 = data[data["unit_Price"] > 100000]
        y_2 = len(df_2)
        y_2_3 = len(df_2_3)
        y_3_4 = len(df_3_4)
        y_4_5 = len(df_4_5)
        y_5_6 = len(df_5_6)
        y_6_7 = len(df_6_7)
        y_7_8 = len(df_7_8)
        y_8_9 = len(df_8_9)
        y_9_10 = len(df_9_10)
        y_10 = len(df_10)
        x = list(range(1, 11))
        _x = ["2万以下", "2万到3万", "3万到4万", "4万到5万", "5万到6万", "6万到7万", "7万到8万", "8万到9万", "9万到10万", "10万以上"]
        y = [y_2, y_2_3, y_3_4, y_4_5, y_5_6, y_6_7, y_7_8, y_8_9, y_9_10, y_10]
        plt.figure(figsize=(20, 8))
        plt.bar(x, y, color="aqua")
        plt.xticks(x, _x, fontproperties=my_font)
        plt.title("该城市单价分布直方图", fontproperties=my_font)
        plt.yticks(y, fontsize=8)
        plt.xlabel("房价单价(元/平方米)", fontproperties=my_font)
        plt.ylabel("数量", fontproperties=my_font)
        plt.show()






    def scatter_pic(self,data):
        pass





    def linksmain(self):
        city = self.city()
        self.get_totalnum(city)
        region_urls = self.get_region(city)[0]
        regions = self.get_region(city)[1]
        Bregion_url=self.get_region_url(city, region_urls, regions)
        wtfspider=main_spiders()
        wtfspider.main(Bregion_url)
        print("爬取完成")
        print("数据已保存成csv文件,如果乱码,请用WPS打开")
        print("进入分析模式")
        self.prompt()
        self.analyse()



class main_spiders(Linkspider):
    @retry(stop_max_attempt_number=3)
    def url2html(self, url):
        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'}
        try:
            response = requests.get(url, headers=headers)
            html = etree.HTML(response.text)
            return html
        except UnicodeDecodeError as e:
            print(e)



    def get_max(self,url):
        html=self.url2html(url)
        maxpage0 = html.xpath('//div[@class="page-box house-lst-page-box"]/@page-data')
        maxpage=str(maxpage0[0])
        maxnum = int(re.findall('(\d+)', maxpage)[0])
        return maxnum

    def make_page_url(self,Bregion_url):#构造每一页的url
        for url in Bregion_url:
            maxnum=self.get_max(url)
            if maxnum<100:
                base_urls = [(url + 'pg{}').format(i) for i in range(1,1+maxnum)]
                for base_url in base_urls:
                    yield base_url
            else:
                url_100=url
                all_ssurl = self.get_100_url(url_100)
                for base_url in all_ssurl:
                    yield base_url

    def get_100_url(self,url_100):
        all_ssurl=[]
        first_url=re.findall('((.*?)com)',url_100)[0][0]
        html_100 = self.url2html(url_100)
        ssurls_0 = html_100.xpath('//div[@data-role="ershoufang"]/div[2]/a/@href')
        ssurls=self.get_100_url_1([first_url+i for i in ssurls_0])
        for ssurl in ssurls:
            ssmaxpage=self.get_max(ssurl)
            base_urls = [(ssurl + 'pg{}').format(i) for i in range(1, 1 + ssmaxpage)]
            for base_url in base_urls:
                all_ssurl.append(base_url)
        return all_ssurl

    def get_100_url_1(self,ssurls_0):#用于get_100_url中判断是否存在0,存在0则剔除
        ssurls=[]
        for ssurl in ssurls_0:
            ssurl_html=self.url2html(ssurl)
            num=int(ssurl_html.xpath('//h2[@class="total fl"]/span/text()')[0])
            if num!=0:
                ssurls.append(ssurl)

        return ssurls


    def poolfunc(self,url):#多线程实现保存到数据库

        item={}
        mongo_coll = mongo_db[name]
        html = self.url2html(url)
        titles = html.xpath('//div[@class="info clear"]//div[@class="title"]/a/text()')
        times = html.xpath('//div[@class="followInfo"]//text()')
        detail_urls = html.xpath('//div[@class="info clear"]//div[@class="title"]/a/@href')
        addresses = html.xpath('//div[@class="address"]//a/text()')
        infos = html.xpath('//div[@class="address"]/div/text()')
        total_prices = html.xpath('//div[@class="totalPrice"]/span/text()')
        unit_Prices = html.xpath('//div[@class="unitPrice"]/span/text()')
        for title, time, detail_url, address, info, total_price, unit_Price in zip(titles, times, detail_urls,addresses, infos, total_prices,unit_Prices):
            newObjectId = ObjectId()
            unit_Price = re.findall('(\d+)', unit_Price)[0]
            total_price = re.findall('(\d+)', total_price)[0]
            time = time.replace(" ", "").strip().split("/")[2]
            item["title"] = title
            item["update_time"] = time
            item["url"] = detail_url
            item["unit_Price"] = unit_Price
            item["total_price"] = total_price
            item['_id'] = newObjectId
            mongo_coll.insert(item)
            print(item)

    def to_csv(self):
        uri = "mongodb://admin:woaljs456765!!@120.77.39.227:27017/"
        client = MongoClient(uri)
        collection = client["links"][name]
        data = pd.DataFrame(list(collection.find()), dtype=int)
        data.to_csv('houseprice.csv', index=False, sep=',', encoding="utf-8")

    def main(self,Bregion_url):
        pool=Pool(16)
        base_url_list=[]
        for base_url in self.make_page_url(Bregion_url):
            base_url_list.append(base_url)
        pool.map(self.poolfunc,[i for i in base_url_list])
        self.to_csv()

if __name__ == '__main__':
    spider = Linkspider()
    spider.linksmain()







