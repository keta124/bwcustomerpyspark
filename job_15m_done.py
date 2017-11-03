from pyspark import SparkContext, SparkConf
from collections import namedtuple
from pyspark.sql import HiveContext, Row, SQLContext
import json
import time


sc= SparkContext(appName="Data_job_15m")

class EsReadWrite(object):
        def __init__(self):
                pass

        def readEs(self):
                try:
                        time_now= int(time.time()/900)*900*1000
                	timeBlock = [time_now-1800000,time_now]
                        query_ ={
                	"filtered":{
                                "query": {
                                        "query_string": {
                                                "analyze_wildcard": True,
                                                "query": "\"ts\" AND type:\"varnish_log\""
                                                }
                                        },
                                        "filter": {
                                                "bool": {
                                                        "must": [{
                                                                "range": {
                                                                        "time_write_log": {
                                                                                "gte": timeBlock[0],
                                                                                "lte": timeBlock[1],
                                                                                "format": "epoch_millis"
                                                                        }
                                                                }
                                                        }],
                                                        "must_not": []
                                                }
                                        }
                                }
                	}
                	field_include ="client_ip,bytes,request_time,group_name"
                	query = json.dumps(query_)
                        conf = {
                        "es.nodes":"192.168.142.101",
                        "es.port":"9200",
                        "es.resource":"cdnlog-*/varnish_log",
                	"es.query":query,
                	"es.read.field.include":field_include
                        }
                        es_rdd = sc.newAPIHadoopRDD(
                                inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
                                keyClass="org.apache.hadoop.io.NullWritable", 
                                valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
                                conf=conf)
                	return es_rdd
                except:
                        return []

        def writeEs(self,es_rdd):
                try:
                	es_rdd_ =es_rdd.map(lambda x: ('key', { 'timecheck': x[1],'subnet': x[0], 'percent_low': x[2], 'percent_mid': x[3], 'percent_good': x[4],'total_request': x[5],'datacenter':x[6],'isp_client': 'isp_'+str(x[7])}))
                	es_write_conf ={
                	"es.nodes" : "192.168.142.101",
                	"es.port" : "9200",
                	"es.resource" : "statbw_customerisp-2017.11/statbw_customerisp"
                	}
                	es_rdd_.saveAsNewAPIHadoopFile(
                		path='-',
                		outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
                		keyClass="org.apache.hadoop.io.NullWritable",
                		valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
                		conf=es_write_conf)
                except:
                        print 'Error write ES'
class MapRdd(object):
        def __init__(self):
                pass
        def calcBw(self,row):
                fields = ('clientip','byte','requesttime','group_name','bandwidth')
                Network = namedtuple('Network',fields)
                if row[1] !=0:
                        bw_ =float(float(row[1]) / (float(row[2])*1024*1024))
                        bw = round(bw_,3)
                else:
                        bw = round(1000.3)
                row.append(bw)
                return Network(*row[:5])

        def ipToSubnet(self,ip):
                listip =  map(int,ip.split('.'))
                value = listip[0]*256*256*256+listip[1]*256*256+listip[2]*256+listip[3]
                value_ = value - value % 256
                C = (value_/256)%256
                B = ((value_-C*256)/(256*256))%256
                A = (value_-C*256-B*256*256) / (256*256*256)
                subnet = str(A)+"."+str(B)+"."+str(C)+".0/24"
                return subnet
        def converNullZero(self,row):
                fields = ('subnet_groupname','lower','mid','greater','total')
                Netmask_Customer = namedtuple('Netmask_Customer',fields)
                if row[1] is None:
                        row[1]=0
                if row[2] is None:
                        row[2]=0
                if row[3] is None:
                        row[3]=0
                total = row[2]+row[3]+row[1]
                row[1] = round((100* row[1] /total),2)
                row[2] = round((100* row[2] /total),2)
                row[3] = 100-row[2]-row[1]
                row.append(total)
                return Netmask_Customer(*row[:5])

        def mapEpochTime(self,row):
                fields = ('subnet_groupname','lower','mid','greater','total','timecheck')
                Customer = namedtuple('Customer',fields)
                time_check = int(time.time())*1000
                row.append(time_check)
                return Customer(*row[:6])

class CvsFile(object):
        def __init__(self,path):
                self.path =path

        def readCvsFile(self):
                ip_info = sc.textFile(self.path)
                isp_rdd = ip_info.map(lambda x: x.split(',')).map(lambda x: (x[1],x[0]))
                return isp_rdd


if __name__ == "__main__":
	time_check = int(time.time())*1000
	isp_rdd  =CvsFile("hdfs:///user/root/data/ipasp_done_24.csv").readCvsFile()
        es_read_write = EsReadWrite()
        map_rdd = MapRdd()
	es_rdd = es_read_write.readEs().map(lambda x : [x[1]['client_ip'],x[1]['bytes'],x[1]['request_time'],x[1]['group_name']]).map(map_rdd.calcBw).map(lambda x: (x.clientip,(x.group_name,x.bandwidth)))
	map_subnet_rdd = es_rdd.map(lambda x :((map_rdd.ipToSubnet(x[0]),x[1][0]),x[1][1]))
	map_subnet_rdd.cache()
        # filter by bandwidth
        rdd_lower = map_subnet_rdd.filter(lambda x: x[1]<=0.2).mapValues(lambda x: 1).reduceByKey(lambda a,b:a+b)
        rdd_mid = map_subnet_rdd.filter(lambda x: ( x[1]> 0.2) and (x[1]<=0.4) ).mapValues(lambda x: 1).reduceByKey(lambda a,b:a+b)
        rdd_greater = map_subnet_rdd.filter(lambda x: x[1]>0.4).mapValues(lambda x: 1).reduceByKey(lambda a,b:a+b)
        rdd_join = rdd_lower.leftOuterJoin(rdd_mid).leftOuterJoin(rdd_greater)
	rdd_value = rdd_join.map(lambda x: [x[0],x[1][0][0],x[1][0][1],x[1][1]]).map(map_rdd.converNullZero).filter(lambda x: x.total >50)
	rdd_value_ = rdd_value.map(lambda x : [x.subnet_groupname,x.lower,x.mid,x.greater,x.total]).map(map_rdd.mapEpochTime)
        rdd_value_pair = rdd_value_.map(lambda x:(x.subnet_groupname[0],(x.timecheck,x.lower,x.mid,x.greater,x.total,x.subnet_groupname[1])))
	rdd_map_isp = rdd_value_pair.leftOuterJoin(isp_rdd).map(lambda x:(x[0],x[1][0][0],x[1][0][1],x[1][0][2],x[1][0][3],x[1][0][4],x[1][0][5],x[1][1]))
	map_subnet_rdd.unpersist()
	es_read_write.writeEs(rdd_map_isp)
