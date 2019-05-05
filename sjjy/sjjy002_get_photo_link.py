import scrapy.cmdline


def main():
    scrapy.cmdline.execute(argv=['scrapy', 'crawl', 'jjy', '-a', 'params='+'{"remote_resource": true}'])


if __name__ == '__main__':
    main()