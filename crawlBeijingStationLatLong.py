import requests
import time
import MySQLdb
from multiprocessing import Pool, Queue

def getStationName():
    """获取公交站名称
    :return:
    """
    db = MySQLdb.connect("localhost", "root", "", "busstation", charset="utf8")
    cursor = db.cursor()
    sql = """
    select distinct(station_name) from stationname
    """
    cursor.execute(sql)
    results = cursor.fetchall()
    cursor.close()
    db.close()
    return results

def getExistStation():
    """获得已经写入数据库的公交站数据
    :return:
    """
    db = MySQLdb.connect("localhost", "root", "", "busstation", charset='utf8')
    cursor = db.cursor()

    sql = """
   select distinct(station_name) from station_latlong
    """
    cursor.execute(sql)
    results = cursor.fetchall()
    db.commit()
    cursor.close()
    db.close()
    return results

def writeMySql(queue):
    """向数据库批量写入数据
    :param data: [[],[],[]...]
    :return:
    """
    data = []
    while not queue.empty():
        data.append(queue.get())
    db = MySQLdb.connect("localhost", "root", "", "busstation", charset='utf8')
    cursor = db.cursor()
    sql = """
    INSERT INTO station_latlong(station_name, lat, lng)
    VALUES (%s, %s, %s)
    """
    cursor.executemany(sql, data)
    db.commit()
    cursor.close()
    db.close()
    return

queues = [Queue() for _ in range(5)]

def getStationLatLong(station):
    index = station[0]
    stationName = "北京市" + station[1] + "公交车站"
    urlTemplate = "http://api.map.baidu.com/geocoding/v3/?address={}&output=json&ak=gH16xrIU5FDPIic4I2ZSnam3yUN05mPT&city=北京市'"
    # random.randint(1,5)
    html = requests.get(urlTemplate.format(stationName))

    latLongJson = html.json()['result']
    lat = latLongJson['location']['lat']
    long = latLongJson['location']['lng']
    queue = queues[index % 5]
    queue.put([station[1], lat, long])
    if queue.qsize() == 20: # 最开始的时候可以设置20，到最后几十条数据的时候需要设置为1
        writeMySql(queue)
        time.sleep(3)

if __name__ == '__main__':
    pool = Pool(4)
    stationNames = getStationName()
    stationNames = set(item[0] for item in stationNames)
    existStations = getExistStation()
    existStations = set(item[0] for item in existStations)
    wait_crawl = stationNames - existStations

    stationNames = [[i,v] for i, v in enumerate(wait_crawl)]
    pool.map(getStationLatLong, stationNames)

