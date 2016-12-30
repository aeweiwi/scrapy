# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


import json
import codecs
import csv
from datetime import datetime


class ShocarsPipeline(object):
    keys = [u'لون السيارة', u'نوع الوقود', u'أصل السيارة', u'رخصة السيارة', u'نوع الجير',
            u'الزجاج', u'قوة الماتور', u'عداد السيارة', u'عدد الركاب', u'السعر',
            u'وسيلة الدفع', u'السيارة معروضة', u'أصحاب سابقون', u'اسم المُعلن',
            u'العنوان', u'رقم الهاتف', u'موبايل', u'تاريخ نشر الإعلان', u'تاريخ إنتهاء الإعلان',
            u'المدينه', u'صفحه', u'موديل', u'نوع السياره']

    land_keys = [u'العنوان', u'المساحه', u'السعر', u'تقع على',
                 u'محاطه ب', u'تصلها المياه', u'تصلها الكهرباء',
                 u'صالحه للزراعه', u'صالحه للبناء التجاري',
                 u'صالحه لبناء السكن', u'اسم المُعلن', u'العنوان',
                 u'رقم الهاتف', u'موبايل', u'تاريخ نشر الإعلان',
                 u'تاريخ إنتهاء الإعلان', 'Email',
                 u'المدينه']

    def __init__(self):
        today = datetime.now().strftime('%Y-%m-%d')
        self.ff = codecs.open('out_{0}.csv'.format(today),
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
