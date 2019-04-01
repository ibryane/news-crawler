from __future__ import print_function
from textrank4zh import TextRank4Keyword, TextRank4Sentence
from pymongo import MongoClient
from bson.binary import Binary
import pickle
import zlib
from urllib.parse import urljoin
from time import sleep
import requests
from bs4 import BeautifulSoup
import threading

DEFAULT_TIMEOUT = 10
DEFAULT_MAXPAGE = 1
DEFAULT_DELAY = 6
DEFAULT_MAXTRY = 3
DEFAULT_MAXTHREAD = 2


class Dataprocession(object):
    '''使用textrank4k接口解析中文新闻获取keywords, abstract'''
    def __init__(self, text):
        self.text = text
        self.article = ''
    def process(self, ):
        for i in self.text:
            self.article += i.getText() + '\n'
        self.article = self.article.strip()
        keywords = []
        abstract = []
        ##关键词
        tr4w = TextRank4Keyword()
        tr4w.analyze(text=self.article, lower=True, window=2)
        for item in tr4w.get_keywords(4, word_min_len=1):
            keywords.append(item.word)
        ##摘要
        tr4s = TextRank4Sentence()
        tr4s.analyze(text=self.article, lower=True, source = 'all_filters')
        for item in tr4s.get_key_sentences(num=3):
            abstract.append(item.sentence)
        return keywords, abstract



class Mongocache:
    def __init__(self, client=None):
        self.client = MongoClient('localhost', 27017) if client is None else client
        self.db = self.client.cache

        
    def __getitem__(self, url):
        ##获取信息
        record = self.db.news.find_one({'_id': url})
        if record:
            return record['result']
        else:
            raise KeyError(url + ' does not exist')
    def __setitem__(self, url, result):
        ##存储信息，由于压缩过程有递归深度错误，没有压缩
        record = {'result': result}
        self.db.news.update_one({'_id': url}, {'$set': record}, upsert=True)



class Download(object):
    """下载内容"""
    def __init__(self, timeout=None, cache=None, max_try=0): 
        self.cache = cache
        self.timeout = timeout
        self.cache = cache
        self.max_try = max_try
        
    def __call__(self, url):
        result = None
        print(url)
        try:
            result = self.cache[url]
        except KeyError:
            pass
        if result is None:
            result = self.start(url)
            if self.cache:
                self.cache[url] = result
        return result
        
    def start(self, url, headers=None, proxy=None,):
        #代理,头设置
        if proxy:
            scheme = proxy.split(':')[0]
            proxies = {scheme: proxy}
        else:
            proxies = None
        if headers is None:
            headers = {'user-agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'}
        while self.max_try > 0:
            try:
                res = requests.get(url, headers=headers, proxies=proxies, timeout=self.timeout)
            except Exception:
                self.max_try -= 1
            else:
                break
        html = res.content
        soup = BeautifulSoup(res.content)
        ##请求访问解析网页
        content = soup.find('div', class_='WYSIWYG articlePage').findAll('p')
        for i in content:
            if i.getText().startswith('【'):
                content.remove(i)
        content1 = ''
        for i in content:
            content1 += str(i) + '\n'
        content1 = content1.strip()
        title = soup.find('h1').string
        ##获取网页中新闻文本和title
        keywords, abstract = Dataprocession(content).process()
        ##获取关键词和摘要
        
        return {'content': content1, 'title': title, 'keywords': keywords, 'abstract': abstract}




class Crawler(object):
    def __init__(self, start_url, category, timeout=DEFAULT_TIMEOUT, max_threads=DEFAULT_MAXTHREAD, max_try=DEFAULT_MAXTRY, max_page=DEFAULT_MAXPAGE, delay=DEFAULT_DELAY, cache=Mongocache()):
        self.start_url = start_url
        self.category = category        
        self.page = []
        self.max_page = max_page
        self.timeout = timeout
        self.delay = delay
        self.links = []
        self.max_try = max_try
        self.cache = cache
        self.max_threads = max_threads
        self.a = Download(timeout=self.timeout, cache=self.cache, max_try=self.max_try)
        
    def parse_first(self, url=None, headers=None, proxy=None,):
        ##爬取网页页面url
        if not url:
            url = self.start_url + self.category
        if proxy:
            scheme = proxy.split(':')[0]
            proxies = {scheme: proxy}
        else:
            proxies = None
        if headers is None:
            headers = {'user-agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'}
        
        res = requests.get(url, headers=headers, proxies=proxies, timeout=self.timeout)
        sleep(self.delay)
        self.page.append(url)
        html = res.content
        soup = BeautifulSoup(html)
        next_link = soup.find('div', {'class': 'sideDiv inlineblock text_align_lang_base_2'}).find('a').attrs['href']
        print('download %s'%url)
        self.max_page -= 1
        while self.max_page > 0:
            if next_link:
                next_link = urljoin(self.start_url, next_link)                
                print('we done one')
                self.parse_first(url=next_link)

    def parse_next(self, headers=None, proxy=None):
        ##爬取网页每页的新闻url
        self.parse_first()
        if proxy:
            scheme = proxy.split(':')[0]
            proxies = {scheme: proxy}
        else:
            proxies = None
        if headers is None:
            headers = {'user-agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'}
        
        for i in self.page:
            res1 = requests.get(i, headers=headers, proxies=proxies, timeout=self.timeout)
            sleep(self.delay)
            html1 = res1.content
            soup1 = BeautifulSoup(html1)
            links1 = soup1.find('div', class_='largeTitle').find_all('article', class_='js-article-item')
            for i in links1:
                link2 = urljoin(self.start_url, i.find('a').attrs['href'])
                self.links.append(link2)
        return self.links
    
    def storage(self,):
        ##存储到数据库
        self.parse_next()
                    
        threads = []
        while threads or self.links:
            for thread in threads:
                if not thread.is_alive():
                    threads.remove(thread)
            while len(threads) < self.max_threads and self.links:
                thread = threading.Thread(target=self.process_queue)
                thread.setDaemon(True)
                thread.start()
                threads.append(thread)
            sleep(self.delay)
    def process_queue(self,):
        while True:
            try:
                url = self.links.pop()
            except IndexError:
                break
            else:
                self.a(url)

if __name__ == '__main__':
    start_url = 'https://cn.investing.com/news'
    cate = '/stock-market-news'
    b = Crawler(start_url, cate)
    b.storage()