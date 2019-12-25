import requests
import pickle
import time
import random
from bs4 import BeautifulSoup
import MySQLdb
from multiprocessing import Pool

def readData(filePath):
    """读取pickle文件
    :param filePath: 文件路径
    :return:
    """
    with open(filePath, 'rb') as f:
        return pickle.load(f)

def writeData(filePath, data):
    """将data写入到filePath
    :param filePath: 路径
    :param data: 数据
    :return:
    """
    with open(filePath, 'wb') as f:
        pickle.dump(data, f)

def writeMySql(data):
    """向数据库批量写入数据
    :param data: [[],[],[]...]
    :return:
    """
    db = MySQLdb.connect("localhost", "root", "", "busstation", charset='utf8')
    cursor = db.cursor()
    sql = """
    INSERT INTO stationname(line_name, url, station_name)
    VALUES (%s, %s, %s)
    """
    cursor.executemany(sql, data)
    db.commit()
    cursor.close()
    db.close()
    return

def getExistLines():
    """获得已经写入数据库的公交站数据
    :return:
    """
    db = MySQLdb.connect("localhost", "root", "", "busstation", charset='utf8')
    cursor = db.cursor()

    sql = """
    select distinct(line_name) from stationname;
    """
    cursor.execute(sql)
    results = cursor.fetchall()
    db.commit()
    cursor.close()
    db.close()
    return results

baseUrl = 'https://beijing.8684.cn'

def getBusStationName(line):
    """获取每条线路line的公交车站名然后写入数据库
    :param line: 公交线路名称
    :return:
    """
    result = []
    tempUrl = baseUrl + line[1]
    try:
        # 随机休眠1-5秒，防止被拒绝
        time.sleep(random.randint(1, 5))
        html = requests.get(tempUrl)
    except:
        # 休眠20秒
        time.sleep(20)
        getBusStationName(line)
    soup = BeautifulSoup(html.text, "html.parser")
    liTag = soup.select(".bus-lzlist")[0].find_all("li")
    for li in liTag:
        result.append([line[0], line[1], li.text])
    writeMySql(result)
    print(line[0], line[1])
    return


if __name__ == '__main__':
    wait_crawl = []
    existLines = getExistLines()
    existLines = [item[0] for item in existLines]
    busLinesPd = readData('./data/busLinesPd.pkl')
    # 将已经爬取的线路排除，爬取未爬取的数据，我本机需要运行这个函数两次，第一次爬取了一千八百多个线路，第二次爬取一百多个。一共一千九百多个线路
    for item in busLinesPd.values:
        if item[0] in existLines:
            continue
        wait_crawl.append([item[0], item[1]])
    p = Pool(4)
    p.map(getBusStationName, wait_crawl)

