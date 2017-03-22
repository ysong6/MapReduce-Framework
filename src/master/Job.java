package master;

import Util.*;
import conf.JobConf;
import log.LogSys;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;


public class Job implements Serializable {
    private static final long serialVersionUID = 1;
    private Integer jobId;
    private String serviceIPaddress;
    private Integer servicePort;
    private String inputFile;
    private String outputFile; //output file path should be created according to inputFile
    private UserMethod partitionMethod;
    private UserMethod mapperMethod;
    private UserMethod reducerMethod;
    private JobStatus status;
    private int partitionSize;
    private ArrayList<MTask> mTasks;
    private ArrayList<RTask> rTasks;
    private Hashtable<Integer, ArrayList<MTask>> completeMappers;//<workerId,taskList>
    private Hashtable<Integer, ArrayList<RTask>> completeReducers;//<workerId,taskList>
    private int NumOfCompleteReducers;
    private int NumOfCompleteMappers;

    public static enum JobState {
        RUNNING, SUCCEEDED, FAILED, UNASSIGNED, KILLED,
        COMMIT_PENDING, FAILED_UNCLEAN, KILLED_UNCLEAN, SUBMITTING
    }

    /**
     * This method contains two parts: start the job and monitor the job's status
     */
    public Job(JobConf jobConf) {
        serviceIPaddress = jobConf.serviceIp;
        servicePort = jobConf.servicePort;
        inputFile = jobConf.inputFile;
        this.outputFile = jobConf.outputFile; //if we need the outputFilePath here?
        this.partitionMethod = jobConf.partitionMethod;
        partitionSize = jobConf.partitionSize;
        this.mapperMethod = jobConf.mapperMethod;
        this.reducerMethod = jobConf.reducerMethod;
        status = new JobStatus();
        mTasks = new ArrayList<>();
        rTasks = new ArrayList<>();
        completeMappers = new Hashtable<>();
        completeReducers = new Hashtable<>();
        NumOfCompleteReducers = 0;
        NumOfCompleteMappers = 0;
    }

    // this method will run on user's client
    public void run() throws IOException {

        JobStatus res = null;

        JobTrackerInterface jobtracker = null;
        Registry registry = null;

        //step 1: Get the Remote Object Reference from JobTracker
        try {
            registry = LocateRegistry.getRegistry(serviceIPaddress, servicePort);
            jobtracker = (JobTrackerInterface) registry.lookup("JobTracker");

        } catch (RemoteException | NotBoundException e) {
            LogSys.err("Register remote service fails");
            return;
        }

        //step 2: submit job to master
        try {
            res = jobtracker.submit(this, IO.readBinaryFile(mapperMethod.classFilePath), IO.readBinaryFile(reducerMethod.classFilePath));
        } catch (IOException e) {
            LogSys.err(e.toString());
            e.printStackTrace();
        }

        // deal with the job result, get the final result file from master.
        switch (res.state) {
            case FAILED:
//                jobtracker.terminateJob(jobId);
                LogSys.log("Job submit failed");
                return;
            case RUNNING:
                LogSys.log("New Job " + res.jobId + " starts");
                break;
            default:
                break;
        }

        jobId = res.jobId;
        int count = 1000;
        // waiting for the whole job finish, just waiting. should we block the thread here?
        while (true) {

            if (count-- == 0) {
                LogSys.log("Job " + jobId + " time out");
                return;
            }

            JobStatus status = jobtracker.getJobStatus(jobId);
            switch (status.state) {
                case RUNNING:
                    LogSys.log("Job " + jobId + " is running");
                    break;
                case SUCCEEDED:
                    try {
                        saveResult(status);
                        SimpleDateFormat format =  new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
                        String startTime = new java.sql.Timestamp(status.startTime).toString();
                        String endTime = new java.sql.Timestamp(status.endTime).toString();
                        long last = status.endTime - status.startTime;
                        last /= 1000;
                        LogSys.log("Job " + jobId + " is completed, which started at "+startTime +" ended at "+endTime+" last: "+last +"s");
                        return;
                    } catch (IOException e) {
                        LogSys.log("Save result file failed because of IO error");
                        LogSys.err(e.toString());
                    }
                    break;
                case FAILED:
                    LogSys.log("Job " + jobId + " failed");
                    break;
            }

            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                System.err.println("Exception happened when monitoring!");
            }
        }
    }

    //split job into multiple task
    public void jobPartition() {
        File f = new File(inputFile);
        long len = f.length();
        int mTaskNum = ((int) len - 1) / partitionSize + 1;
        int rTaskNum = (int) (1.75 * (Monitor.getAliveWorkerNum() == 0 ? 4 : Monitor.getAliveWorkerNum()));

        //create all reducerMethod task
        for (int i = 0; i < rTaskNum; i++) {
            RTask rtask = new RTask(jobId, i, mTaskNum, jobId + "_" + i + new SimpleDateFormat().format(new Date()) + ".txt", reducerMethod.userClass.getName(), null);
            rTasks.add(rtask);
        }

        //create all mapperMethod task
        for (int i = 0; i < mTaskNum; i++) {
            int l = partitionSize;
            if (i == mTaskNum - 1) {
                l = (int) (len % partitionSize);
            }
            MapperInput mIn = new MapperInput(inputFile, i * partitionSize, l, partitionMethod);
            MTask mtask = new MTask(jobId, i, mIn, rTaskNum, mapperMethod, partitionMethod);
            mTasks.add(mtask);
        }


    }

    //derive output file from the job result
    public void saveResult(JobStatus status) throws IOException {

        OutputCollector out = new OutputCollector();
        for (byte[] byteArray : status.output) {
            String str = new String(byteArray);
            String lines[] = str.split("\n");
            for (String line : lines) {
                String words[] = line.split(",");
                if (words.length != 2) {
                    LogSys.err("wrong format in reducerMethod result");
                    continue;
                }
                KvPair<String, String> pair = new KvPair<>(words[0], words[1]);
                out.outputCollector.add(pair);
            }
        }

        String filename = generateOutputFilePath();
        int index = filename.length() - 1;
        while (index >= 0 && filename.charAt(index) != '/') {
            index--;
        }
        String dir = filename.substring(0, index);

        File fileDir = new File(dir);
        if (!fileDir.exists()) {
                fileDir.mkdirs();

        }

        File file = new File(filename);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                LogSys.err("write result file fail");
            }
        }

        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter(file);
            while (!out.outputCollector.isEmpty()) {
                KvPair<String, String> pair = out.outputCollector.poll();
                fileWriter.write(pair.getKey() + "," + pair.getValue() + "\n");
            }
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String generateOutputFilePath() {

        String out = outputFile + jobId;
        return out;
    }

    public void setJobStatus(int id, JobState state) {
        status.jobId = id;
        status.state = state;
    }

    public void setJobStatus(JobState state) {
        status.state = state;
    }

    public JobStatus getJobStatus() {
        return status;
    }

    public void setStartTime(long t) {
        status.startTime = t;
    }

    public void setEndTime(long endTime) {
        status.endTime = endTime;
    }

    public long getEndTime() {
        return status.endTime;
    }

    public long getStartTime() {
        return status.startTime;
    }

    public int getJobId() {
        return jobId;
    }

    public void setJobId(int id) {
        jobId = id;
    }

    public String getMapperPath() {
        return mapperMethod.classFilePath;
    }

    public void setMapperPath(String path) {
        mapperMethod.classFilePath = path;
    }

    public String getReducerPath() {
        return reducerMethod.classFilePath;
    }

    public void setReducerPath(String path) {
        reducerMethod.classFilePath = path;
    }

    public ArrayList<MTask> getmTasks() {
        return mTasks;
    }

    public ArrayList<RTask> getrTasks() {
        return rTasks;
    }

    public void setMRClassPath() {
        mapperMethod.classFilePath = "./mrclass/_" + jobId + "_mapper.class";
        reducerMethod.classFilePath = "./mrclass/_" + jobId + "_reducer.class";
    }

    public MTask getMapperTask(int taskId) {
        return mTasks.get(taskId);
    }

    public RTask getReducerTask(int taskId) {
        return rTasks.get(taskId);
    }

    public Iterator<MTask> getAllMapperTask() {
        return mTasks.iterator();
    }

    public ArrayList<RTask> getAllReducerTask() {
        return rTasks;
    }

    public Hashtable<Integer, ArrayList<MTask>> getCompleteMappers() {
        return completeMappers;
    }

    public Hashtable<Integer, ArrayList<RTask>> getCompleteReducers() {
        return completeReducers;
    }

    public int getNumOfCompleteReducers() {
        return NumOfCompleteReducers;
    }

    public int getNumOfCompleteMappers() {
        return NumOfCompleteMappers;
    }

    public void setNumOfCompleteReducers(int numOfCompleteReducers) {
        NumOfCompleteReducers = numOfCompleteReducers;
    }

    public void setNumOfCompleteMappers(int numOfCompleteMappers) {
        NumOfCompleteMappers = numOfCompleteMappers;
    }

    public void updateData4rTaskFinished(int taskId) {
        RTask task = rTasks.get(taskId);
        task.updateState(Task.TaskState.SUCCEEDED);
        ArrayList<RTask> completeRTasks = completeReducers.get(task.workerId);
        if (completeRTasks == null) {
            completeRTasks = new ArrayList<>();
            completeReducers.put(task.workerId, completeRTasks);
        }
        completeRTasks.add(task);
        NumOfCompleteReducers++;
    }


    public boolean isJobFinished() {
        return (rTasks.size() - NumOfCompleteReducers) == 0;
    }

    public void updateJobCompeletInfo(byte[] result[]) {

        status.output = result;
        status.state = JobState.SUCCEEDED;

        //clear worker info
        Scheduler.clearTaskInfo(this.jobId);

    }
}
