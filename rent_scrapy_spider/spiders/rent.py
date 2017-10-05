# -*- coding: utf-8 -*-
import scrapy

import sqlite3
import time
import os
import csv
from bs4 import BeautifulSoup
from scrapy import Selector
from scrapy.http import Request, FormRequest
from rent_scrapy_spider.items import RentScrapySpiderItem

import rent_config
import rent_util


class RentSpider(scrapy.Spider):
    name = 'rent'
    allowed_domains = ['douban.com']

    def __init__(self):
        # 创建目录
        path_name = 'results'
        # results_path = os.path.join(sys.path[0], path_name)
        this_file_dir = os.path.split(os.path.realpath(__file__))[0]
        results_path = os.path.join(this_file_dir, path_name)
        if not os.path.isdir(results_path):
            os.makedirs(results_path)

        # 解析配置文件
        config_file_path = os.path.join(this_file_dir, 'rent_config.ini')
        self.config = rent_config.Config(config_file_path)

        # 初始化一些变量
        self.douban_header = {
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36"}
        # self.douban_cookie = {}

        self.login_url = 'https://accounts.douban.com/login'
        self.login_form_data = {
            # 'redir': 'https://www.douban.com',
            'form_email': self.config.douban_login_email,
            'form_password': self.config.douban_login_password,
            'login': u'登陆'
        }

        # 附带时间的文件名
        file_time_format = '%Y%m%d_%X'
        file_time = time.strftime(file_time_format, time.localtime()).replace(':', '')
        self.result_file_name = results_path + '/result_' + str(file_time)
        # 数据库文件名
        self.result_sqlite_name = self.result_file_name + '.sqlite'
        # 网页文件名
        self.result_html_name = self.result_file_name + '.html'

        self.result_rent_list_name = self.result_file_name + '_rent_list.csv'
        self.result_rent_detail_name = self.result_file_name + '_rent_detail.csv'

        self.create_table_sql = 'CREATE TABLE IF NOT EXISTS rent(id INTEGER PRIMARY KEY, title TEXT, url TEXT UNIQUE,' \
                                'user_name TEXT, content TEXT, city TEXT, area TEXT, address TEXT, contact TEXT,' \
                                'last_updated_time TEXT, craw_time TEXT)'
        self.insert_sql = 'INSERT INTO rent(id, title, url, user_name, content, city, area, address, contact,' \
                          'last_updated_time, craw_time) ' \
                          'VALUES(NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
        self.select_detail_link_sql = 'SELECT url FROM rent'
        # self.select_complete_data_sql = 'SELECT url, title, content, area, address, contact FROM rent'
        self.update_content_sql = 'UPDATE rent SET content = ?, city = ? WHERE url = ?'
        # self.update_content_other_sql = 'UPDATE rent SET area = ?, address = ?, contact = ? WHERE url = ?'
        self.select_sql = 'SELECT * FROM rent ORDER BY last_updated_time DESC ,craw_time DESC'

        self.list_urls = self.get_urls()

    @staticmethod
    def get_urls():
        # 分页查找（豆瓣当前是按每页 25 条显示的）
        list_urls = ['https://www.douban.com/group/145219/discussion?start=' + str(i) for i in range(0, 50, 25)]
        list_urls_sh = ['https://www.douban.com/group/homeatshanghai/discussion?start=' + str(i) for i in
                        range(0, 50, 25)]
        list_urls.extend(list_urls_sh)
        return list_urls

    @staticmethod
    def get_city_type(url):
        city = 1
        if 'https://www.douban.com/group/145219/' in url:
            city = 1
        elif 'https://www.douban.com/group/homeatshanghai/' in url:
            city = 2

        return city

    def start_requests(self):
        return [Request(
            url=self.login_url,
            meta={"cookiejar": 1},
            headers=self.douban_header,
            callback=self.post_login)]

    def post_login(self, response):
        if response.status == 200:
            captcha_url = response.xpath('//*[@id="captcha_image"]/@src').extract()  # 获取验证码图片的链接
            if len(captcha_url) > 0:
                print 'manual input captcha, link url is: %s' % captcha_url
                captcha_text = raw_input('Please input the captcha:')
                self.login_form_data['captcha-solution'] = captcha_text
            else:
                print 'no captcha'
            print 'login processing......'

            return [
                FormRequest.from_response(
                    response,
                    meta={"cookiejar": response.meta["cookiejar"]},
                    headers=self.douban_header,
                    formdata=self.login_form_data,
                    callback=self.after_login
                )
            ]

        else:
            print 'request login page error %s -status code: %s:' % (self.login_url, response.status)

    def after_login(self, response):
        if response.status == 200:
            title = response.xpath('//title/text()').extract()[0]
            if u'登录豆瓣' in title:
                print 'login failed, please retry!'
            else:
                print 'login success!'

                for i in range(len(self.list_urls)):
                    url = self.list_urls[i]
                    print 'list page url: %s' % url
                    yield Request(url=url, headers=self.douban_header, callback=self.parse)

        else:
            print 'request post login error %s -status code: %s:' % (self.login_url, response.status)

    def parse(self, response):
        if response.status == 200:
            sel = Selector(response)
            sites = sel.xpath('//table[@class="olt"]/tr[@class=""]')
            for site in sites:
                item = RentScrapySpiderItem()

                last_updated_time = site.xpath('td[@nowrap="nowrap"][3]/text()').extract()
                if last_updated_time:
                    item['last_updated_time'] = last_updated_time[0]
                    # print item['last_updated_time']

                # if item['last_updated_time']:
                #     last_updated_timestamp = rent_util.Util.get_time_from_str(item['last_updated_time'])
                #     start_timestamp = rent_util.Util.get_time_from_str(self.config.start_time)
                #     # 时间检查（结束的条件是配置文件里设置的起始时间）
                #     if last_updated_timestamp < start_timestamp:
                #         print 'crawl list data at end, stop'
                #         break

                title = site.xpath('td[@class="title"]/a')
                if title:
                    item['title'] = title.xpath('@title').extract()[0]
                    item['url'] = title.xpath('@href').extract()[0]
                    # print item['title']
                    # print item['url']

                name = site.xpath('td[@nowrap="nowrap"][1]/a')
                if name:
                    item['user_name'] = name.xpath('text()').extract()[0]
                    # print item['user_name']

                # check data
                if item['title'] and item['url'] and item['user_name'] and item['last_updated_time']:
                    item['craw_time'] = rent_util.Util.get_time_now()
                    item['content'] = ''
                    item['area'] = ''
                    item['address'] = ''
                    item['contact'] = ''
                    # process item
                    yield item

                    # 请求详情页
                    yield Request(url=item['url'], headers=self.douban_header, callback=self.parse_detail)

        else:
            print 'request list page error %s -status code: %s:' % (response.url, response.status)

    def parse_detail(self, response):
        if response.status == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            topic_content = soup.find_all(attrs={'class': 'topic-content'})[1]
            group_item = soup.find_all(attrs={'class': 'group-item'})[0]
            if topic_content and group_item:
                try:
                    content_array = []
                    result_array = topic_content.find_all(['p', 'img'])
                    for item in result_array:
                        # print item.name
                        if item.name == 'p':
                            for br in item.find_all('br'):
                                br.replace_with('\n')
                            content_array.append(item.text)
                        if item.name == 'img':
                            img_src = item.get('src')
                            content_array.append(img_src)
                    content = ','.join(content_array)

                    group_url = group_item.find_all('a')[0].get('href')
                    city = self.get_city_type(group_url)

                    item = RentScrapySpiderItem()
                    item['url'] = response.url
                    item['content'] = content
                    item['city'] = city

                    print response.url
                    # print content
                    # print 'city:%d' % city

                    # process item
                    yield item
                except Exception, e:
                    print 'topic content parse error:', e
            else:
                print 'detail page topic content is null'
        else:
            print 'request detail page error %s -status code: %s:' % (response.url, response.status)

    def generate_csv(self):
        # 导出 cvs 格式文件
        conn = sqlite3.connect(self.result_sqlite_name)
        conn.text_factory = str
        cursor = conn.cursor()
        cursor.execute(self.select_sql)
        values = cursor.fetchall()

        print '========== Check whether the data is complete =========='
        total_count = len(values)
        empty_count = 0
        for row in values:
            if not str(row[1]) or not str(row[2]) or not str(row[3]) or not str(row[4]) or not str(row[5]) or not str(row[9]):
                empty_count += 1
                print '[item is empty]: %s' % str(row[2])
        print 'check result item total count: %d, empty count: %d' % (total_count, empty_count)

        print '========== Begin output CVS file =========='
        rent_list_file = open(self.result_rent_list_name, 'wb')
        writer_list = csv.writer(rent_list_file)
        writer_list.writerow(['title', 'url', 'city', 'area', 'last_updated_time'])

        # (id, title, url, user_name, content, city, area, address, contact, last_updated_time, craw_time)
        for row in values:
            writer_list.writerow([str(row[1]), str(row[2]), str(row[5]), str(row[6]), str(row[9])])
        rent_list_file.close()

        rent_detail_file = open(self.result_rent_detail_name, 'wb')
        writer_detail = csv.writer(rent_detail_file)
        writer_detail.writerow(
            ['title', 'url', 'user_name', 'content', 'city', 'area', 'address', 'contact', 'last_updated_time'])

        # (id, title, url, user_name, content, city, area, address, contact, last_updated_time, craw_time)
        for row in values:
            writer_detail.writerow(
                [str(row[1]), str(row[2]), str(row[3]), str(row[4]), str(row[5]), str(row[6]), str(row[7]),
                 str(row[8]), str(row[9])])
        rent_detail_file.close()

        cursor.close()
        print '========== Output CVS file end! =========='
        print '========================================='

    def close(self, reason):
        print 'spider close, reason: %s' % reason
        if reason == 'finished':
            print 'Rent Spider Finish!'
            self.generate_csv()
            print 'Done Success!'
