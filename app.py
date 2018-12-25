from pyspark import SparkContext, SparkConf
import sys
import re
import cassandra.cluster as cascl
from cassandra import ConsistencyLevel
from tempfile import NamedTemporaryFile
import cassandra

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
    
def SaveToDB(CollectedRdd,cluster,KeyspaceName='syslog',TableName='statistics'):
    '''
    '''
    session = cluster.connect(KeyspaceName)
    record = session.prepare("INSERT INTO {} (hour, priority, amount) VALUES (?, ?, ?)".format(TableName))
    batch = cassandra.query.BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    for (key,value) in CollectedRdd:
        (p,h)=divmod(key,100) #key = priority*100+hour
        batch.add(record, (h,p,value))
    session.execute(batch)
    session.shutdown()

def CreateKeySpaceAndTable(cluster,KeyspaceName='syslog',TableName='statistics'):
    '''
    '''
    session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION \
                    = { 'class' : 'SimpleStrategy','replication_factor' : 1 }" % (KeyspaceName,))
    session.set_keyspace(KeyspaceName)
    session.execute("CREATE TABLE IF NOT EXISTS {} \
                    (hour tinyint, priority tinyint, amount int, \
                    PRIMARY KEY (hour,priority))".format(TableName)) 
    session.shutdown()


def printFromDb(cluster,KeyspaceName='syslog',TableName='statistics',file=sys.stdout):
    '''
    '''
    session = cluster.connect(KeyspaceName)
    rows = session.execute("SELECT hour, priority, amount FROM {}".format(TableName))
    session.shutdown()
    p = ['emerg','alert','crit','error','warning','notice','info','debug']
    res = []
    for row in rows:
        print(row.hour,p[row.priority],row.amount,file=file)
        res.append((row.hour,row.priority,row.amount))
    return res

def SparkCalculate(context,filename,tmpFile):
    '''
    '''
    rdd = context.textFile(filename) #create rdd from log file which contains 
                                   #lines like: "%PRI%: %timegenerated% %HOSTNAME%
                                   #%syslogtag%%msg:::drop-last-lf%\n"
    rdd = rdd.map(LineMap).sortByKey().reduceByKey(lambda a,b: a+b) # logline->(key,1)->sort->(key,amount)
    if rdd.isEmpty():
        return None   #TODO raise Exeption
    oldrdd = context.pickleFile(tmpFile.name)
    if oldrdd.isEmpty():
        newrdd = rdd
    else:
        newrdd = oldrdd.union(rdd).sortByKey().reduceByKey(lambda a,b: a+b) # oldrdd U rdd->sort->(key,amount)
    tmpFile = NamedTemporaryFile(delete=True)
    tmpFile.close()
    newrdd.saveAsPickleFile(tmpFile.name)
    open(filename, "w")  #remove all logs from logfile
    result = newrdd.collect()
    return result


if __name__ == '__main__':
    if len(sys.argv) !=2:
        print ('Usage: app.py <logfile>')
        sys.exit(-1)

    KeyspaceName = 'syslog'
    TableName = 'statistics'
    cluster = cascl.Cluster()
    CreateKeySpaceAndTable(cluster,KeyspaceName,TableName)
    conf = SparkConf().setAppName('CountingSyslogsByHours')
    sc = SparkContext(conf=conf)
    tmpFile = NamedTemporaryFile(delete=True)
    tmpFile.close()
    sc.emptyRDD().saveAsPickleFile(tmpFile.name)
    statistics = SparkCalculate(sc,sys.argv[1],tmpFile=tmpFile)
    #writing to Cassandra
    SaveToDB(statistics,cluster)
    #printing from Cassandra
    printFromDb(cluster)
    cluster.shutdown()
    sc.stop()


    