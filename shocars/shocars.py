# -*- coding: utf-8 -*-

from scrapy.utils.project import get_project_settings
import scrapy
from selenium import webdriver
from scrapy.crawler import CrawlerProcess
from urlparse import urljoin
from pandas.compat import u
from scrapy.spiders import CrawlSpider
from scrapy.spiders import Rule
from scrapy.http import Request
import pandas as pd
from scrapy.linkextractors import LinkExtractor
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor


class shocars(CrawlSpider):
    name = "shocars"
    keys = [u'لون السيارة', u'نوع الوقود', u'أصل السيارة', u'رخصة السيارة', u'نوع الجير',
            u'الزجاج', u'قوة الماتور', u'عداد السيارة', u'عدد الركاب', u'السعر',
            u'وسيلة الدفع', u'السيارة معروضة', u'أصحاب سابقون', u'اسم المُعلن',
            u'العنوان', u'رقم الهاتف', u'موبايل', u'تاريخ نشر الإعلان', u'تاريخ إنتهاء الإعلان',
            u'المدينه', u'صفحه', u'موديل', u'نوع السياره']
    # allowed_domains = ['http://shobiddak.com']
    start_urls = ['http://shobiddak.com/cars']

    rules = (Rule(SgmlLinkExtractor(allow=(),
                                    restrict_xpaths=('//a[@class="next_page"]',)),
                  callback='parse_items', follow=True, process_links='process_link'),)

    def process_link(self, links):
        return links

    def parse_items(self, response):

        for link in response.xpath('//p[@class="section_title"]/a/@href').extract():

            url = urljoin(response.url, link)
            yield Request(url, callback=self.parse_content)

    def parse_content(self, response):
        title = re.sub(
            '\s+', ' ', response.xpath('//h1[@class="section_title"]/text()').extract()[0])
        model = re.sub(
            '\s+', ' ', response.xpath('//h3[@class="section_title"]/text()').extract()[0].split()[2])

        sel = response.xpath('//tr[@class="list-row"]/td/text()')
        att = {}
        i = 1
        while i < len(sel) + 1:
            aa = '(//tr[@class="list-row"]/td/text())[{0}]'.format(i)
            a = response.xpath(aa).extract()[0]
            a = re.sub('\s+', ' ', a).strip()
            i += 1

            if a not in self.keys:
                continue
            if a == u'العنوان':
                i += 1
            bb = '(//tr[@class="list-row"]/td/text())[{0}]'.format(i)
            b = response.xpath(bb).extract()[0]
            b = re.sub('\s+', ' ', b).strip()
            i += 1
            att.update({a: b})
        city_possible = response.xpath(
            '//tr[@class="list-row" and @id="row_1"]/td/a/text()').extract()
        city = city_possible[len(city_possible) - 1]

        att.update({u'المدينه': city})
        att.update({u'صفحه': response.url})
        att.update({u'موديل': model})
        att.update({u'نوع السياره': title})

        for key, val in att.iteritems():
            print '{0}:    {1}'.format(key, val)

        x = 1
        yield att


if __name__ == '__main__':

    try:
        from scrapy.settings import Settings
        import settings as mysettings
        # UTF8Writer = getwriter('utf8')
        # sys.stdout = UTF8Writer(sys.stdout)

        crawler_settings = Settings()
        crawler_settings.setmodule(mysettings)
        process = CrawlerProcess(settings=crawler_settings)

        #ss = get_project_settings()
        #process = CrawlerProcess(ss)
        #import sys

        process.crawl('shocars')
        # process.crawl(shocars)
        # the script will block here until the crawling is finished
        process.start()
    except Exception as e:
        print 'something stupid happened'
