#!/bin/bash
if [ $# -eq 0 ] ; then
echo Usage: script.sh [-i] logfile
exit -1
elif [ $# -eq 1 ]; then
file=$1
elif [ $# -eq 2 ]; then
install=$1
file=$2
fi

if [ -n $SPARK_HOME ] ;then
SPARK_HOME=/media/divine0ff/a1c55a32-f216-42af-b5bd-1ffc4e70a5f0/divine0ff/spark-2.4.0-bin-hadoop2.7
fi

if [[ $install == "-i" ]] ; then
echo installing prerequisites...
#installing and configuring spark
wget https://www.apache.org/dyn/closer.lua/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz
tar -xzf spark-2.4.0-bin-hadoop2.7.tgz
SPARK_HOME=spark-2.4.0-bin-hadoop2.7
#installing and configuring cassandra
echo "deb http://www.apache.org/dist/cassandra/debian 36x main" | sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list
curl https://www.apache.org/dist/cassandra/KEYS | sudo apt-key add -
sudo apt-get update
sudo apt-get install cassandra
sudo service cassandra start
#configuring enviroment
python3 -m venv env
env/bin/pip3 install cassandra-driver
env/bin/pip3 install pyspark
echo ---------------------------------------------
fi

if [ -r $file ] ; then
echo  checking cassandra cluster status...
#sudo service cassandra start
nodetool status
echo ---------------------------------------------
echo configuring rsyslogd...
sudo touch /etc/rsyslog.d/prior_filter.conf
sudo echo \$template TraditionalFormatWithPRI,\"%PRI%: %timegenerated% %HOSTNAME% %syslogtag%%msg:::drop-last-lf%\\n\" >/etc/rsyslog.d/prior_filter.conf
sudo echo "*.* -$file;TraditionalFormatWithPRI">>/etc/rsyslog.d/prior_filter.conf
service rsyslog restart
echo ---------------------------------------------
#echo writing to db ...
#echo ---------------------------------------------
echo submit the app ... 
$SPARK_HOME/bin/spark-submit --master local[*] --conf spark.pyspark.python="env/bin/python" app.py $file
$SPARK_HOME/bin/spark-submit --master local[*] --conf spark.pyspark.python="env/bin/python" app_test.py 
echo ---------------------------------------------
#sudo service cassandra stop
fi
exit 0


   

