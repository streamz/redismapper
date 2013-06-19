redismapper
===========

hadoop job to load redis from hdfs
this job will only load multi-maps using hset<primarykey,hashkey,value>
run with the following commands

required:

-redis <host:port>
-input <PROTOCOL://PATH to CSV>
-key   <primarykey>
-hkey  <hashkey>
-hval  <hashval>

optional:
-kf    <regex that will exclude records with matching primary keys>
-hf    <regex that will exclude records with matching hash keys>
-vf    <regex that will exclude records with matching hash values>

deploy redismapper-1.0-SNAPSHOT-job.jar

example:
a csv file with lines that look as follows:
12345,hello,world,67890,foo,bar,1.37901
23456,hello,world,67890,bad,bar,1.0

hadoop redismapper-1.0-SNAPSHOT-job.jar -redis=localhost:6379 -input=/users/mydata -key=0 -hkey=4 -vkey=6 -hf=^bad -vf=1.0

the following would write 1 of the 2 records to redis in the following format:
hset(12345,foo,1.37901)