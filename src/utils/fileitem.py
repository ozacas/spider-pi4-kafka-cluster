import scrapy

class FileItem(scrapy.Item):
   origin = scrapy.Field()
   file_urls = scrapy.Field()
   files = scrapy.Field()
