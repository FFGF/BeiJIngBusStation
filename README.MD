# BeiJingBusStation
爬取北京市公交车站点经纬度数据，将里面的URL修改一下即可爬取其他地区的公交车站点经纬度

* crawlBeijingBusLineName.py文件爬取线路信息
* crawlBeijingBusStationName.py文件爬取公交车站名
* crawlBeijingStationLatLong.py文件爬取站名的经纬度数据

经纬度来源于百度的地址解析API，精确性不能保证，如果出事概不负责

sqlResult是爬取的数据，存储成了.sql文件

其他信息见[简书](https://www.jianshu.com/p/f18ea9c53a2f)