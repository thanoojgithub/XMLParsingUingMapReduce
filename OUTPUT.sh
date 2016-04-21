ubuntu@ubuntu:~/workspaces/wstwo/MRTwo/target$ hadoop fs -rm -r /output
16/04/21 09:22:18 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
16/04/21 09:22:19 INFO fs.TrashPolicyDefault: Namenode trash configuration: Deletion interval = 0 minutes, Emptier interval = 0 minutes.
Deleted /output
ubuntu@ubuntu:~/workspaces/wstwo/MRTwo/target$ hadoop jar MRTwo-1.0.jar /input/employees.xml /output
16/04/21 09:22:28 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
XmlDriver.main() - input path /input/employees.xml
XmlDriver.main()  - output path /output
16/04/21 09:22:29 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
16/04/21 09:22:29 WARN mapreduce.JobResourceUploader: Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
16/04/21 09:22:29 INFO input.FileInputFormat: Total input paths to process : 1
16/04/21 09:22:29 INFO mapreduce.JobSubmitter: number of splits:1
16/04/21 09:22:30 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1461244817723_0005
16/04/21 09:22:30 INFO impl.YarnClientImpl: Submitted application application_1461244817723_0005
16/04/21 09:22:30 INFO mapreduce.Job: The url to track the job: http://ubuntu:8088/proxy/application_1461244817723_0005/
16/04/21 09:22:30 INFO mapreduce.Job: Running job: job_1461244817723_0005
16/04/21 09:22:36 INFO mapreduce.Job: Job job_1461244817723_0005 running in uber mode : false
16/04/21 09:22:36 INFO mapreduce.Job:  map 0% reduce 0%
16/04/21 09:22:42 INFO mapreduce.Job:  map 100% reduce 0%
16/04/21 09:22:48 INFO mapreduce.Job:  map 100% reduce 100%
16/04/21 09:22:48 INFO mapreduce.Job: Job job_1461244817723_0005 completed successfully
16/04/21 09:22:48 INFO mapreduce.Job: Counters: 49
	File System Counters
		FILE: Number of bytes read=163
		FILE: Number of bytes written=235533
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=638
		HDFS: Number of bytes written=147
		HDFS: Number of read operations=6
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=2
	Job Counters 
		Launched map tasks=1
		Launched reduce tasks=1
		Data-local map tasks=1
		Total time spent by all maps in occupied slots (ms)=3201
		Total time spent by all reduces in occupied slots (ms)=3589
		Total time spent by all map tasks (ms)=3201
		Total time spent by all reduce tasks (ms)=3589
		Total vcore-milliseconds taken by all map tasks=3201
		Total vcore-milliseconds taken by all reduce tasks=3589
		Total megabyte-milliseconds taken by all map tasks=3277824
		Total megabyte-milliseconds taken by all reduce tasks=3675136
	Map-Reduce Framework
		Map input records=5
		Map output records=5
		Map output bytes=147
		Map output materialized bytes=163
		Input split bytes=106
		Combine input records=0
		Combine output records=0
		Reduce input groups=5
		Reduce shuffle bytes=163
		Reduce input records=5
		Reduce output records=5
		Spilled Records=10
		Shuffled Maps =1
		Failed Shuffles=0
		Merged Map outputs=1
		GC time elapsed (ms)=62
		CPU time spent (ms)=1560
		Physical memory (bytes) snapshot=515784704
		Virtual memory (bytes) snapshot=5395869696
		Total committed heap usage (bytes)=383778816
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=532
	File Output Format Counters 
		Bytes Written=147
ubuntu@ubuntu:~/workspaces/wstwo/MRTwo/target$ hadoop fs -ls /output
16/04/21 09:23:00 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Found 2 items
-rw-r--r--   1 ubuntu supergroup          0 2016-04-21 09:22 /output/_SUCCESS
-rw-r--r--   1 ubuntu supergroup        147 2016-04-21 09:22 /output/part-r-00000
ubuntu@ubuntu:~/workspaces/wstwo/MRTwo/target$ hadoop fs -cat /output/part-r-00000
16/04/21 09:23:06 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
1	SRIRAM	21-04-2016	AYODHYA
2	SEETA	21-04-2016	MIDHILA
3	LAKSHMANA	21-03-2016	AYODHYA
4	BHARATHA	21-03-2016	AYODHYA
5	SETHRUGNA	21-02-2016	AYODHYA
ubuntu@ubuntu:~/workspaces/wstwo/MRTwo/target$
