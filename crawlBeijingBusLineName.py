import requests
import pandas as pd
import pickle
import os

from bs4 import BeautifulSoup

def readData(filePath):
    with open(filePath, 'rb') as f:
        return pickle.load(f)

def writeData(filePath, data):
    with open(filePath, 'wb') as f:
        pickle.dump(data, f)

baseUrl = 'https://beijing.8684.cn'
urlList = ['1', '2', '3', '4', '5', '6', '7', '8', '9', 'B', 'C', 'D', 'F', 'G', 'H', 'K', 'L', 'M', 'P', 'S', 'T', 'X', 'Y', 'Z']

def getBusLineName():
    """爬取北京市公交车线路名称
    :return:
    """
    result = []
    for url in urlList:
        tempUrl = baseUrl + "/list" + url
        html = requests.get(tempUrl)
        soup = BeautifulSoup(html.text, "html.parser")
        aTag = soup.select(".list > a")
        for a in aTag:
            result.append([a.text, a['href']])
    return result

busLines = getBusLineName()
busLinesPd = pd.DataFrame(columns=['lineName', 'url'], data=busLines)

if os.path.exists('/data/busLinesPd.pkl'):
    writeData('./data/busLinesPd.pkl', busLinesPd)