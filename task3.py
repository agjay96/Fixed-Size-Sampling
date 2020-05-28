from pyspark.streaming import StreamingContext
from pyspark import SparkContext
import sys
import random

seqno=0
hundred=[]

port = int(sys.argv[1])
filename = sys.argv[2]
random.seed(553)
def test(x):

	f = open(filename, "a+")

	global seqno
	global hundred

	x = x.collect()
	for i in x:
		
		if(seqno<100):
			hundred.append(i)
		else:
			r=random.randint(0,100000)
			if (r%seqno)<100:
				hundred[r%seqno]=i
		seqno+=1
		if seqno%100==0:
			f.write("\n"+str(int(seqno))+","+hundred[0]+","+hundred[20]+","+hundred[40]+","+hundred[60]+","+hundred[80])

	f.close()


if __name__ == "__main__":

	sc = SparkContext()
	sc.setLogLevel(logLevel="ERROR")

	scc = StreamingContext(sc, 10)
	streaming_c = scc.socketTextStream("localhost", port)

	f = open(filename, "w+")
	f.write("seqnum,0_id,20_id,40_id,60_id,80_id")
	f.close()

	streaming_c.foreachRDD(test)

	scc.start()
	scc.awaitTermination()