import unittest
import app
import sys
from pyspark import SparkContext, SparkConf


class TestMethods(unittest.TestCase):

  def test_LineMap(self):

      s = '77: Dec 23 23:16:38 divine0ff-Aspire-E1-570G anacron[1024]: Normal exit (1 job run)'
      print(sys.argv)
      self.assertEqual(app.LineMap(s), (523,1))

  def test_spark(self):

      res = app.SparkCalculate('logfile.test')
      print('test computedRdd:',res)
      self.assertCountEqual(res, [(606,1),(511,2),(611,2)])

  def test_saveToDB(self):

      app.SaveToDB([(606,1),(511,2),(611,2)],KeyspaceName='test')
      res=app.printFromDb(file=None,KeyspaceName='test')
      print('test saved table:',res)
      self.assertCountEqual(res, [(6,6,1),(11,6,2),(11,5,2)])


if __name__ == '__main__':
    with open('logfile.test','w') as f:
        f.write("30: Dec 24 06:05:18 divine0ff-Aspire-E1-570G systemd[1]: Mounting /boot/efi...\n\
46: Dec 25 11:54:49 divine0ff-Aspire-E1-570G rsyslogd:  bla bla...\n\
77: Dec 25 11:55:06 divine0ff-Aspire-E1-570G anacron[1001]: bla bla ...\n\
13: Dec 25 11:57:56 divine0ff-Aspire-E1-570G code[3534]: bla bla ...\n\
14: Dec 25 11:58:01 divine0ff-Aspire-E1-570G code.desktop[3534]: bla bla ...")
    unittest.main()