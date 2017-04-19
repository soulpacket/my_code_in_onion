import requests
from lxml import etree
from selenium import webdriver
url = 'https://item.taobao.com/item.htm?spm=a230r.1.14.97.rYtIPU&id=544534050370&ns=1&abbucket=10#detail'
# a = requests.get(url)
# selector = etree.HTML(a.text)
# content = selector.xpath("//ul[@data-property='颜色分类']")
# print(content)
driver = webdriver.Chrome('/Users/root_1/Downloads/chromedriver')  # 打开浏览器
driver.get(url)  # 打开url
# path = "*[@data-property='颜色分类'/li[1]/a/span]"
# path = "//ul[@data-property = '颜色分类']/li[1]"
# path = '//*[@id="J_DetailMeta"]/div[1]/div[1]/div/div[4]/div/div/dl[2]/dd/ul/li[1]/a/span'
# path = "//div[@class='tb-btn-add']"
# path = "//div[@class='tb-btn-basket tb-btn-sku ']"
path = "//div[@id='J_SureSKU']/p[1]"
a = driver.find_element_by_xpath(path)
print(a.text)


