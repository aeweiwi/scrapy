# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


import json
import codecs
import csv
from datetime import datetime


class SholandsPipeline(object):

    keys = [u'العنوان', u'المساحة', u'السعر', u'تقع على', u'منطقه',
            u'محاطه ب', u'تصلها المياه', u'تصلها الكهرباء',
            u'صالحه للزراعه', u'صالحه للبناء التجاري',
            u'صالحه لبناء سكن', u'اسم المُعلن', u'العنوان',
            u'رقم الهاتف', u'موبايل', u'تاريخ نشر الإعلان',
            u'تاريخ إنتهاء الإعلان', 'Email', u'صفحه']

    def __init__(self):
        today = datetime.now().strftime('%Y-%m-%d')
        self.ff = codecs.open('out_land_{0}.csv'.format(today),
                              'w',
                              encoding='utf-8')
        line = ','.join(self.keys)
        line += '\n '

        self.ff.write(line)

    def spider_closed(self, spider):
        self.ff.close()

    def process_item(self, item, spider):

        line = ''
        for i, k in enumerate(self.keys):
            if k in item:
                line += item[k]
            else:
                line += '-'

            if i != len(self.keys) - 1:
                line += ','

        line += '\n '
        self.ff.write(unicode(line))
