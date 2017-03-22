This project implements a basic MapReduce framework, which supports part of fundamental MapReduce features, including 
1.How to build the project
This project includes three main components: Master, Worker and JobClient. These three components run independently and concurrently, so this project should be built into three jar packages: master.jar, worker.jar and job.jar, as following:
•Master.jar:
Master.jar runs in master node, which coordinate whole MapReduce process. The main class of it is “master.JobTracker”. 
•Worker.jar
Worker.jar runs in multiple worker (slave) nodes, which executes mapper task and reducer task. The main class of it is “worker.TaskTracker”. 
•Job.jar
Job.jar runs in client’s server. It defines the user’s job and submit the job into master. The main class of it is “client.WordCount”.

2.How to setup the project
2.1 Configuration file
This project use configuration files to provide necessary information to these three components. There are three configuration files as following:
•mapreduce.conf:
This configuration file is only for master. It defines the service IP address of master, service port of JobTracker module, which responsible for track all jobs and communicate with the job client, service port of Monitor module, which responsible for monitoring the framework’s status through heartbeat, the interval of heartbeat.

Example:
masterServiceIpAddress = 10.211.55.2
jobTrackerServicePort = 9000
monitorServicePort = 8000
heartBeatInterval = 1000

•slave.conf:
This configuration file is only for worker. It defines master’s service IP, master’s service port, master’s service name, the file path of intermediate file, partition file and result file of reduce task, the resource of worker, including the number of mapper slot and reducer slot, interval of heartbeat and service name of worker.

Example:
masterServiceIP = 10.211.55.2
masterServicePort = 8000
masterServiceName = Monitor
mapperSlotNum = 3
reducerSlotNum = 2
partitionFilePath = ./result/reducer/intermediateResults/
intermediateFilePath = ./result/mapper/
reducerOutput = ./result/reducer/result/
heartBeatInterval = 1000
slaveServiceName = TaskTracker

•job.conf:
•This configuration file is only for job client. It defines master’s service IP, master’s service port, the file path of mapper method and reducer method, input file and final result, partition size, job client’s IP and interval of job status checking

Example:
mapperMethodPath = ./methods/WordcountMapper.class
reducerMethodPath = ./methods/WordcountReducer.class
partitionSize = 10240
inputFile = ./source/wordcount.txt
outputFile = ./result/finalResult/
serviceIp = 10.211.55.5
servicePort = 9000
jobClientIp = 10.211.55.5
heartBeatInterval = 1000

2.2 Steps of setup environment
1.copy jar package to corresponding servers. You should copy master.jar to master, worker.jar to workers, jobClient.jar to job client server.
2.Set up the necessary directories and configuration files for these three components:
•Master
Total two directories should be set up in master, which are “conf” and “source”.  The “mapreduce.conf” should be put into “conf” directory and input source file should put into “source” directory.
•Worker
Total two directories should be set up in master, which are “conf” and “source”.  The “slave.conf” should be put into “conf” directory and input source file should put into “source” directory.
•Job client
Total two directories should be set up in master, which are “conf” and “methods”.  The “job.conf” should be put into “conf” directory and the methods file of mapper and reducer should be put into “methods” directory.
3.Complete the configuration files based on the environment such as the IP address of master, workers and job client.

2.3 Execute the project through CLI
•Master
 java -jar master.jar
•Worker
java -jar worker.jar <service IP of this worker> <service port of this worker>
•Job client
java -jar jobClient.jar
