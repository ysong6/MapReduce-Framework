package worker;

import Util.*;
import conf.ConfigPath;
import conf.Configuration;
import log.LogSys;
import master.*;
import java.io.IOException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.*;

/**
 * This class is used for managing tasks on a specific worker.
 */

public class TaskTracker extends UnicastRemoteObject implements TaskTrackerInterface {
    //private static JobTrackerInterface jobTracker = null;
    private static MonitorInterface monitor;
    private static TaskTracker taskTracker;

    /**
     * Create a thread pool for mapperMethod threads
     */
    private static ExecutorService mapperExecutor = Executors.newCachedThreadPool();
    /**
     * Create a thread pool for reducerMethod threads
     */
    private static ExecutorService reducerExecutor = Executors.newCachedThreadPool();
    private String masterServiceIP;
    private Integer masterServicePort;
    private String masterServiceName;
    private String selfServiceIpAdd;
    private Integer selfServicePort;
    private static int workerId;
    private static int mapperSlotNum;
    private static int reducerSlotNum;
    private HashMap<Integer, String> mapperToolkit;
    private HashMap<Integer, String> reducerToolkit;
    private static ConcurrentLinkedDeque<TaskEvent> eventQueue = new ConcurrentLinkedDeque<>();
    private static Hashtable<String, LinkedBlockingQueue> reducerPool;
    private static HashMap<Integer, HashMap<Integer, String[]>> allIntermediateFiles;  // <JobId,<TaskId,FileNames>>
    private static String reducerOutput;
    private int heartBeatInterval;
    public static String partitionFilePath;
    public static String slaveServiceName;
    public static String intermediateFilePath;



    public TaskTracker(String serviceIP, int servicePort) throws RemoteException {
        selfServiceIpAdd = serviceIP;
        selfServicePort = servicePort;
        mapperToolkit = new HashMap<>();
        reducerToolkit = new HashMap<>();
        reducerPool = new Hashtable<>();
        allIntermediateFiles = new HashMap<>();
    }

    @Override
    public void allocateTask(Task task) throws RemoteException {
        if(task == null) return;

        int jobId = task.getJobId();
        /** If mapperMethod and reducerMethod class does not exist, download the mapperMethod and reducerMethod file to local. */
        //check task type
        if (task instanceof MTask) {
            String mapperMethod = ((MTask) task).getMapperName();
            if (mapperToolkit.containsKey(jobId)) {
                fetchMapMethod(jobId, mapperMethod);
                //TODO: download partition class?
                mapperToolkit.put(jobId, mapperMethod);
            }

            mapperTaskStart((MTask) task);

        } else {
            String reduceMethod = ((RTask) task).getReduceMethod();
            if (!reducerToolkit.containsKey(jobId)) {
                fetchReduceMethod(jobId, reduceMethod);
                reducerToolkit.put(jobId, reduceMethod);
            }
            reducerTaskStart((RTask) task);
        }
    }

    /**
     * Download the mapperMethod class
     */
    private void fetchMapMethod(int jobId, String mapperName) {
        String mapperClassName = mapperName.replace('.', '/') + ".class";
//        Get Mapper File
        byte[] mapperClassContent = new byte[0];
        try {
            mapperClassContent = monitor.getMapperClass(workerId, jobId);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            IO.writeBinaryFile(mapperClassContent, mapperClassName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Download the reducerMethod class
     */
    private void fetchReduceMethod(int jobID, String reducerName) {
        String reducerClassName = reducerName.replace('.', '/') + ".class";

        try {
            byte[] reducerClassContent = monitor.getReducerClass(workerId, jobID);
            IO.writeBinaryFile(reducerClassContent, reducerClassName);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * This function is called by the master to register and start one mapperMethod task.
     */
    public void mapperTaskStart(MTask task) throws RemoteException {
        LogSys.log("mapperMethod "+task.getTaskId()+"start");

        MapperThread mapRunner = new MapperThread(task);
        mapperExecutor.execute(mapRunner);
        mapperSlotNum--;
    }

    /**
     * This function is called by the master to register and start one reducerMethod task.
     */
    public void reducerTaskStart(RTask rTask) {
        LogSys.log("reducerMethod "+rTask.getTaskId()+"start");
        ReducerThread reducerThread = new ReducerThread(rTask);
//        reducerPool.put(Integer.toString(rTask.getJobId()) + "-" + Integer.toString(rTask.getTaskId()), reducerThread);
        reducerExecutor.execute(reducerThread);
    }

    @Override
    public void mapperCompleteEventNotify(MapperCompleteEvent event) {
        try {
            LinkedBlockingQueue<MapperCompleteEvent> eventQueue = reducerPool.get(event.getJobId() + "-" + event.getrTaskId());
            if(eventQueue == null){
                eventQueue = new LinkedBlockingQueue<MapperCompleteEvent>();
                reducerPool.put(event.getJobId() + "-" + event.getrTaskId(),eventQueue);
                LogSys.debug("reducerMethod "+ event.getrTaskId() +" receive mapperMethod "+ event.getrTaskId()+ " event earlier");
            }
            eventQueue.put(event);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * map taskId to number of partitions to fetch
     */
    HashMap<Integer, Integer> jobPartitions = new HashMap<>();
    /**
     * map taskId to list of partition file names
     */
    HashMap<Integer, ArrayList<String>> partitionPaths = new HashMap<>();

    /**
     * Add the event to the queue for transmitting the heartbeat
     */
    public synchronized static void addEvent(TaskEvent event) {
        eventQueue.add(event);
    }

    private void heartBeatTimer() {
        TimerTask timer = new TimerTask() {
            @Override
            public void run() {
                ArrayList<TaskEvent> transmitQueue = new ArrayList<>();
                synchronized (eventQueue) {
                    if (!eventQueue.isEmpty()) {
                        for (int i = 0; i < eventQueue.size(); i++) {
                            transmitQueue.add(eventQueue.getFirst());
                            eventQueue.removeFirst();
                        }
                    }
                }
                try {
                    monitor.heartBeat(workerId, transmitQueue);
                } catch (IOException e) {
                    LogSys.err("Heart beat sent failed");
                }
            }
        };
        new Timer().scheduleAtFixedRate(timer, 0, heartBeatInterval);

    }

    public static void main(String[] args) {

        try {
            taskTracker = new TaskTracker(args[0],Integer.valueOf(args[1]));

            LogSys.setLogSys(taskTracker.selfServiceIpAdd, "Worker ");

            Configuration.configParser(ConfigPath.slaveConf,taskTracker);
            LogSys.log("Starting TaskTracker...");
            /* TODO: Read job configuration ???*/

            LogSys.log("Connecting to master...");

            Registry registry = LocateRegistry.getRegistry(taskTracker.masterServiceIP, taskTracker.masterServicePort);
            monitor = (MonitorInterface) registry.lookup("Monitor");

        } catch (RemoteException | NotBoundException e) {
            LogSys.err("Connecting to master failed");
            e.printStackTrace();
            return;
//            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            String url = "rmi://"+taskTracker.selfServiceIpAdd+":"+taskTracker.selfServicePort+"/"+taskTracker.slaveServiceName;
            Registry registry = LocateRegistry.createRegistry(taskTracker.selfServicePort);
            Naming.rebind(url, taskTracker);

            //TODO: Get IP address

            workerId = monitor.workerRegister(taskTracker.selfServiceIpAdd, taskTracker.selfServicePort, mapperSlotNum, reducerSlotNum);

            LogSys.setLogSys(taskTracker.selfServiceIpAdd , "worker "+ workerId);


            LogSys.log("I'm worker "+workerId);
            /** transmit heartbeat */
            taskTracker.heartBeatTimer();


        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public static void addIntermediateFiles(int jobId, int taskId, String files[]) {
        LogSys.debug("add allIntermediateFiles job " +jobId+" task "+taskId);

        synchronized (allIntermediateFiles) {
            HashMap<Integer, String[]> interMediateFiles = allIntermediateFiles.get(jobId);
            if(interMediateFiles == null){
                interMediateFiles = new HashMap<>();
            }
            interMediateFiles.put(taskId, files);
            allIntermediateFiles.put(jobId, interMediateFiles);
        }
    }

    //get intermediate files for reducerMethod task
    public static String getIntermediateFilePath(int jobId, int taskId, int partitionId){
        LogSys.debug("get job" + jobId + "'s task " + taskId + "'s partition " + partitionId + "'s path");
        synchronized (allIntermediateFiles) {
            HashMap<Integer, String[]> map = allIntermediateFiles.get(jobId);
            if (map == null) {
                LogSys.err("intermediate result is null");
            }
            if(map.get(taskId) == null){
                LogSys.err("intermediate result of " + taskId + "is null" );
            }
            return map.get(taskId)[partitionId];
        }
    }


    public byte[] getIntermediateResult(int rWorkerId, int jobId, int mTaskId, int rTaskId) throws IOException {

        LogSys.debug("get job" + jobId + "'s task " + mTaskId + "'s partition " + rTaskId + "'s path "
                + "from worker " + rWorkerId);

        String file = getIntermediateFilePath(jobId, mTaskId, rTaskId);

        try {
            return IO.readBinaryFile(file);
        } catch (IOException e) {
            LogSys.err("Get intermediate result fail for reducerMethod task " + rTaskId);
            throw e;
        }

    }

    @Override
    public byte[] getResultOfReducer(int rWorkerId, int jobId, int rTaskId) throws IOException {

        String file = getResultPathOfReducer(jobId, rTaskId);

        try {
            return IO.readBinaryFile(file);
        } catch (IOException e) {
            LogSys.err("Get result fail for reducerMethod task " + rTaskId);
            throw e;
        }
    }

    public static byte[] getLocalIntermediateResult(int jobId, int mTaskId, int rTaskId) throws IOException {
        LogSys.debug("get job" + jobId + "'s task " + mTaskId + "'s partition " + rTaskId + "'s path "
                + "from local ");

        String file = getIntermediateFilePath(jobId, mTaskId, rTaskId);

        try {
            return IO.readBinaryFile(file);
        } catch (IOException e) {
            LogSys.err("Get intermediate result fail for reducerMethod task " + rTaskId);
            throw e;
        }

    }

    public static String getReducerOutput() {
        return reducerOutput;
    }

    public static Hashtable<String, LinkedBlockingQueue> getReducerPool() {
        return reducerPool;
    }

    public String getResultPathOfReducer(int jobId, int taskId){
        return reducerOutput + jobId + "/" + taskId;
    }

}
