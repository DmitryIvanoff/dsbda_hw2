from pyspark import SparkContext, SparkConf
import sys
import re
import cassandra.cluster as cascl


def LineMap(line):
    '''
        Func extracts priority and hour of log 
        with format like: "%PRI%: %timegenerated% %HOSTNAME%
        #%syslogtag%%msg:::drop-last-lf%\n"
    '''
    res = line.split(':')
    pri = int(res[0])%8
    hour = int((res[1].split(' '))[-1])
    return (pri*100+hour,1)
    
def SaveToDB(CollectedRdd,KeyspaceName='syslog',TableName='statistics'):
    '''
    '''
    cluster = cascl.Cluster()
    session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION \
                    = { 'class' : 'SimpleStrategy','replication_factor' : 1 }" % (KeyspaceName,))
    session.set_keyspace(KeyspaceName)
    session.execute("CREATE TABLE IF NOT EXISTS {} \
                    (hour tinyint, priority tinyint, amount int, \
                    PRIMARY KEY (hour,priority))".format(TableName)) 
    for (key,value) in CollectedRdd:
        (p,h)=divmod(key,100) #key = priority*100+hour
        session.execute("INSERT INTO {} (hour, priority, amount) VALUES (%s, %s, %s)"
                       .format(TableName),(h,p,value))   
    cluster.shutdown()

def printFromDb(file=sys.stdout,KeyspaceName='syslog',TableName='statistics'):
    '''
    '''
    cluster = cascl.Cluster()
    session = cluster.connect(KeyspaceName)
    rows = session.execute("SELECT hour, priority, amount FROM {}".format(TableName))
    cluster.shutdown()
    p = ['emerg','alert','crit','error','warning','notice','info','debug']
    res = []
    for row in rows:
        print(row.hour,p[row.priority],row.amount,file=file)
        res.append((row.hour,row.priority,row.amount))
    return res

def SparkCalculate(filename):
    '''
    '''
    conf = SparkConf().setAppName('CountingSyslogsByHours')
    sc = SparkContext(conf=conf)
    rdd = sc.textFile(filename) #create rdd from log file which contains 
                                   #lines like: "%PRI%: %timegenerated% %HOSTNAME%
                                   #%syslogtag%%msg:::drop-last-lf%\n"
    result = rdd.map(LineMap).sortByKey().reduceByKey(lambda a,b: a+b).collect()
    sc.stop()
    return result


if __name__ == '__main__':
    if len(sys.argv) !=2:
        print ('Usage: app.py <logfile>')
        sys.exit(-1)
    statistics = SparkCalculate(sys.argv[1])
    #writing to Cassandra
    SaveToDB(statistics)
    printFromDb()


    