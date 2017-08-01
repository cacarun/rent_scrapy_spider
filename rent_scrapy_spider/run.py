from scrapy import cmdline

name = 'rent'
cmd = 'scrapy crawl {0}'.format(name)
cmdline.execute(cmd.split())
