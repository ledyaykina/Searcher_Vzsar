from vzsar.items import VzsarItem
import scrapy
from urllib.parse import urljoin


class VzsarSpider(scrapy.Spider):
    name = "vzsar"
    start_urls = ['https://www.vzsar.ru/articles/?page=1']
    visited_urls = []

    def parse_post(self, response):
        item = VzsarItem()
        item['title'] = response.xpath('//div[@class="articlehead newshead"]/h1/text()').extract()
        item['body'] = response.xpath('//div[@class="full"]/p/text()').extract()
        item['date'] = response.xpath('//div[@class="articlehead newshead"]/p[1]/text()').extract()
        item['url'] = response.url
        yield item

    def parse(self, response):
        if response.url not in self.visited_urls:
            self.visited_urls.append(response.url)
        for post_link in response.xpath('//div[@class="newslist loadContainer"]/div[@class="main"]/a/@href').extract():
            url = urljoin(response.url, post_link)
            yield response.follow(url, callback=self.parse_post)

        next_pages = response.xpath('//li[contains(@class, "page-item") and'
                                    ' not(contains(@class, "active"))]/a/@href').extract()
        next_page = next_pages[-1]

        next_page_url = urljoin(response.url + '/', next_page)
        yield response.follow(next_page_url, callback=self.parse)