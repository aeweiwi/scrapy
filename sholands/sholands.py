# -*- coding: utf-8 -*-

from scrapy.utils.project import get_project_settings
import scrapy
from selenium import webdriver
from scrapy.crawler import CrawlerProcess
import codecs
from urlparse import urljoin
from pandas.compat import u
from scrapy.spiders import CrawlSpider
from scrapy.spiders import Rule
from scrapy.http import Request
import pandas as pd
from scrapy.linkextractors import LinkExtractor
import sys
reload(sys)


from scrapy.settings import Settings
import settings as mysettings

from kitchen.text.converters import getwriter

sys.setdefaultencoding("utf-8")

import re
import numpy
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor


class sholands(CrawlSpider):
    name = "sholands"
    keys = [u'العنوان', u'المساحة', u'السعر', u'تقع على',
            u'محاطه ب', u'تصلها المياه', u'تصلها الكهرباء',
            u'صالحه للزراعه', u'صالحه للبناء التجاري',
            u'صالحه لبناء سكن', u'اسم المُعلن', u'العنوان',
            u'رقم الهاتف', u'موبايل', u'تاريخ نشر الإعلان',
            u'تاريخ إنتهاء الإعلان', 'Email',
            u'المدينه']

    # allowed_domains = ['http://shobiddak.com']
    start_urls = ['http://shobiddak.com/lands']

    rules = (Rule(SgmlLinkExtractor(allow=(),
                                    restrict_xpaths=('//a[@class="next_page"]',)),
                  callback='parse_items', follow=True, process_links='process_link'),)

    def process_link(self, links):
        return links

    def parse_items(self, response):
        land_xpath_link = '//table[@class="list_ads"]/*[@id="row_0"]/td[1]/a/@href'

        for link in response.xpath(land_xpath_link).extract():

            url = urljoin(response.url, link)

            yield Request(url, callback=self.parse_content)

    def parse_content(self, response):

        title = response.xpath(
            '//h1[@class="section_title"]/text()').extract()[0]

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
            b = ''
            if a == u'العنوان':

                city_region_link = '//tr[@class="list-row"]/td/a/text()'
                region_name = u'منطقه'
                city = ''
                region = ''
                xregion = ''
                try:
                    city = response.xpath(city_region_link).extract()[0]
                    region = response.xpath(city_region_link).extract()[1]

                    tmp = '//tr[@class="list-row"]/td/text()[{0}]'
                    xregion = response.xpath(tmp.format(i + 1)).extract()[0]
                    xregion = re.sub('\s+', ' ', xregion).strip()
                    if xregion != u'':
                        i += 1
                except Exception as e:
                    pass

                region = u'{0}_{1}'.format(region, xregion)
                att.update({a: city})
                att.update({region_name: region})
            else:
                bb = '(//tr[@class="list-row"]/td/text())[{0}]'.format(i)

                b = response.xpath(bb).extract()[0]
                b = re.sub('\s+', ' ', b).strip()
                att.update({a: b})
            i += 1

        att.update({u'صفحه': response.url})

        for key, val in att.iteritems():

            print '{0}: {1}'.format(key.encode('utf-8'), val.encode('utf-8'))

        yield att


if __name__ == '__main__':

    UTF8Writer = getwriter('utf8')
    sys.stdout = UTF8Writer(sys.stdout)

    crawler_settings = Settings()
    crawler_settings.setmodule(mysettings)
    process = CrawlerProcess(settings=crawler_settings)

    # ss = get_project_settings()
    # process = CrawlerProcess(ss)
    # import sys

    process.crawl('sholands')
    # process.crawl(sholands)
    # the script will block here until the crawling is finished
    process.start()
