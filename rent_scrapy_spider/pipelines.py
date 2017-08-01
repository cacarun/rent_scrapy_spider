# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import sqlite3


class RentScrapySpiderPipeline(object):

    def open_spider(self, spider):
        print 'RentScrapySpiderPipeline -> open_spider create db'
        # create database
        self.conn = sqlite3.connect(spider.result_sqlite_name)
        self.conn.text_factory = str
        self.cursor = self.conn.cursor()
        self.cursor.execute(spider.create_table_sql)

    def close_spider(self, spider):
        print 'RentScrapySpiderPipeline -> close_spider'
        self.conn.close()

    def process_item(self, item, spider):
        if item:
            try:
                if item['content'] == '':
                    # save item
                    self.cursor.execute(spider.insert_sql,
                                        [item['title'],
                                         item['url'],
                                         item['user_name'],
                                         '',  # content
                                         '',  # city
                                         '', '', '',
                                         item['last_updated_time'],
                                         item['craw_time']
                                         ])
                    self.conn.commit()
                    print 'list page save item...'

                else:
                    # update item
                    self.cursor.execute(spider.update_content_sql,
                                        [item['content'],
                                         item['city'],
                                         item['url']])
                    self.conn.commit()
                    print 'detail page update content...'

            except sqlite3.Error, e:
                print 'list page save item error:', e

