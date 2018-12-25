import unittest
import app
import sys
from pyspark import SparkContext, SparkConf
import cassandra.cluster as cascl




class TestMethods(unittest.TestCase):

  conf = SparkConf().setAppName('TestCountingSyslogsByHours')
  sc = SparkContext(conf=conf)
  cluster = cascl.Cluster()
  KeyspaceName = 'test'
  TableName = 'statistics'
  tmpFile = app.NamedTemporaryFile(delete=True)
  tmpFile.close()

  def setUp(self):
    app.CreateKeySpaceAndTable(cluster,KeyspaceName,TableName)
    sc.emptyRDD().saveAsPickleFile(tmpFile.name)
    tmpFile.close()

  def test_LineMap(self):

    s = '77: Dec 23 23:16:38 divine0ff-Aspire-E1-570G anacron[1024]: Normal exit (1 job run)'
    print(sys.argv)
    self.assertEqual(app.LineMap(s), (523,1))

  def test_spark(self):

    res = app.SparkCalculate(sc,'logfile.test',tmpFile)
    print('test computedRdd:',res)
    self.assertCountEqual(res, [(606,1),(511,2),(611,2)])

  def test_saveToDB(self):

    app.SaveToDB([(606,1),(511,2),(611,2)],cluster,KeyspaceName=KeyspaceName,TableName=TableName)
    res=app.printFromDb(cluster,file=None,KeyspaceName=KeyspaceName,TableName=TableName)
    print('test saved table:',res)
    self.assertCountEqual(res, [(6,6,1),(11,6,2),(11,5,2)])

def tearDown(self):

    cluster.shutdown()
    sc.stop()

if __name__ == '__main__':
    
    with open('logfile.test','w') as f:
        f.write("30: Dec 24 06:05:18 divine0ff-Aspire-E1-570G systemd[1]: Mounting /boot/efi...\n\
46: Dec 25 11:54:49 divine0ff-Aspire-E1-570G rsyslogd:  bla bla...\n\
77: Dec 25 11:55:06 divine0ff-Aspire-E1-570G anacron[1001]: bla bla ...\n\
13: Dec 25 11:57:56 divine0ff-Aspire-E1-570G code[3534]: bla bla ...\n\
14: Dec 25 11:58:01 divine0ff-Aspire-E1-570G code.desktop[3534]: bla bla ...")
    unittest.main()
