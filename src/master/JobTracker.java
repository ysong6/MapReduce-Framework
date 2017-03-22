package master;

import Util.IO;
import conf.ConfigPath;
import conf.Configuration;
import log.LogSys;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;

//this class
//data: job list
public class JobTracker extends UnicastRemoteObject implements JobTrackerInterface {
    private static HashMap<Integer, Job> jobs;
    private static Integer nextJobId;
    public static String masterServiceIpAddress;
    private int jobTrackerServicePort;

    public static enum JobState {Fail, Succeed, Running}

    public JobTracker() throws RemoteException {
        nextJobId = 0;
        jobs = new HashMap<>();
//        super();
    }

    public static void main(String args[]) throws IOException {

        try {
            //1. start JobTracker
            JobTracker jobTracker = new JobTracker();
            JobTrackerInterface stub = null;

            try {
                Configuration.configParser(ConfigPath.MRConf, jobTracker);
                jobTracker.startJobTracker();

            } catch (FileNotFoundException e) {
                LogSys.err("MRconf file is not found");
            } catch (IOException e) {
                LogSys.err(e.toString());
            }

            //2. initialize Log system
            LogSys.setLogSys(jobTracker.masterServiceIpAddress, "Master");
            LogSys.log("Starting JobTracker...");

            //3. start scheduler thread
            Scheduler scheduler = new Scheduler();
            scheduler.startScheduler();
            LogSys.log("Starting Scheduler...");
            //4. start monitor
            Monitor monitor = new Monitor();
            monitor.startMonitor();
            LogSys.log("Starting Monitor...");

            LogSys.log("Master initiates complete \n");
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }

    public void startJobTracker() throws IOException {

        try {
            Configuration.configParser(ConfigPath.MRConf, this);
            String url = "rmi://"+JobTracker.masterServiceIpAddress+":"+jobTrackerServicePort+"/JobTracker";
            Registry registry = LocateRegistry.createRegistry(jobTrackerServicePort);
            Naming.rebind(url, this);

        } catch (FileNotFoundException e) {
            LogSys.err("MRconf file is not found");
        } catch (IOException e) {
            LogSys.err(e.toString());

        }
    }


    //run in master node
    public JobStatus submit(Job job, byte[] mapper, byte[] reducer) {
        //step 1: initiateJob

        try {
            initiateJob(job);

        } catch (FileNotFoundException e) {
            LogSys.err(job.getJobId() + "Input file not found ");
        } catch (IOException e) {
            LogSys.err(e.toString());
        }

        LogSys.debug("New Job "+ job.getJobId() + " start");

        //step 2: Get mapperMethod and reducerMethod class
        try{
            createMRClass(job, mapper, reducer);
        }catch (IOException e){
            LogSys.err("Create Mapper and Reducer Class error: "+ e.toString());
        }

        // step 3: split job into tasks. should do it later.
//        job.jobPartition();

        //step 4: notify new job submit event to Scheduler
        JobEvent njob = new JobEvent(Event.EventType.Job_Create, job);
        Scheduler.EventNotify(njob);
        JobStatus res = new JobStatus(job.getJobId(), Job.JobState.RUNNING);

        return res;
    }

    public JobStatus getJobStatus(int jobId) throws IOException{
        LogSys.debug("get job Status");
        return jobs.get(jobId).getJobStatus();
    }

    public void initiateJob(Job job) throws IOException {

        //step 1: initiate job's info and data structure

        //assign jobId for the new job.
        int jobId = 0;
        synchronized (nextJobId) {
            jobId = nextJobId;
            nextJobId++;
        }
        job.setJobId(jobId);
        job.setJobStatus(jobId, Job.JobState.RUNNING);
        job.setStartTime(System.currentTimeMillis());
        job.setMRClassPath();
        jobs.put(jobId, job);
        // step 2: Initiate all task for job

    }

    public static void startMonitor() {

        Scheduler scheduler = new Scheduler();

        try {
            Configuration.configParser(ConfigPath.MRConf, scheduler);

        } catch (FileNotFoundException e) {
            LogSys.err("MRconf file is not found");
        } catch (IOException e) {
            LogSys.err(e.toString());
        }

        Thread thread = new Thread(scheduler);
        thread.start();
    }

    public void createMRClass(Job job , byte[] mapper, byte[] reducer) throws IOException {

        IO.writeBinaryFile(mapper, job.getMapperPath());
        IO.writeBinaryFile(reducer,job.getReducerPath());

    }

    public static Job getJob(int jId){
        return jobs.get(jId);
    }

    public static HashMap<Integer, Job> getJobs() {
        return jobs;
    }
}
