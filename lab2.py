# Lab1 submission by MD Shamiul Haque / IA-626 / Fall 2019
# Submission Date: 10th September 2019
'''
ip address
year 
month 
day
hour
minute
second
UTC offset 
HTTP error code 200

KEYS of the Data

connection:0
    ip
        octate:0
        octate:1
        octate:2
        octate:3
    dateinfo
        date
            year
            month
            day
        time
            hour
            minure
            second
        uts
            value
            sign
        rawstring
        PARSEOK?
'''
    
import pymysql # run pip install pymysql if this fails
import csv
import time

start = time.time()

with open('wifi_list.csv') as f:
    wifiTable = [{k: str(v) for k, v in row.items()}
        for row in csv.DictReader(f, skipinitialspace=True)]

print "Import : " + str(time.time() - start)

conn = pymysql.connect(host='mysql.clarksonmsda.org', port=3306, user='ia626', \
                       passwd='ia626clarkson', db='ia626', autocommit=True) #setup our credentials
cur = conn.cursor(pymysql.cursors.DictCursor)

cur.execute("TRUNCATE TABLE `mdhaque_wifi`")
i=0



sql=''
tokens = []
n= 0

sql = '''INSERT INTO mdhaque_wifi (`MAC`,`SSID`,`AuthMode`,`FirstSeen`, 
    `Channel`,`RSSI`,`CurrentLatitude`,`CurrentLongitude`,`AltitudeMeters`,`Type`
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'''
blocksizes = [1000]
for bs in blocksizes:
    start = time.time()
    for row in wifiTable:
        dtl = row['FirstSeen'].split(' ')
        dts = '0000-00-00 00:00'
        if len(dtl) == 2:
            dl = dtl[0].split("/")
            dts = dl[2] + '/' + dl[0] + '/'+dl[1] + ' ' +dtl[1] 
        tokens.append((row["MAC"],row["SSID"],row["AuthMode"],dts, \
        row["Channel"],row["RSSI"],row["CurrentLatitude"],row["CurrentLongitude"],\
        row["AltitudeMeters"],row["Type"]))
        #print conn.insert_id()
        if i % bs == 0:
            n+=1
            #print tokens
            bstart = time.time()
            cur.executemany(sql,tokens)
            conn.commit()
            #print "Block " + str(n) +" "+ str(time.time() - bstart)
            tokens = []
        i+=1
    print "block size: " + str(bs) + " - total time : " + str(time.time() - start)
    if len(tokens) > 0:
        cur.executemany(sql,tokens)
        conn.commit()
    
cur.close()
conn.close()