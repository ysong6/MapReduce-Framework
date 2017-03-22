package worker;

import Util.*;
import log.LogSys;
import master.Event;
import master.MapperCompleteEvent;
import master.RTask;
import master.TaskEvent;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This class is used to start a thread from thread pool to run reducerMethod task.
 */

public class ReducerThread implements Runnable {

    private RTask task;
    /**
     * Mapping jobId and taskId to its path for reducerMethod reading the input files
     */
    private int workerId;
    private int jobId;
    private int taskId;
    private int numOfintemidateFileLeft;
    /**
     * File name of reducerMethod output
     */
    private LinkedBlockingQueue<MapperCompleteEvent> eventQueue;
    private String intermediateFiles[];
    /**
     * @param rTask
     */
    public ReducerThread(RTask rTask) {
        this.workerId = rTask.getWorkerId();
        this.jobId = rTask.getJobId();
        this.taskId = rTask.getTaskId();
        this.numOfintemidateFileLeft = rTask.getNumOfintemidateFile();
        this.eventQueue = TaskTracker.getReducerPool().get(jobId + "-" + taskId);
        if(eventQueue == null){
            eventQueue = new LinkedBlockingQueue<MapperCompleteEvent>();
            TaskTracker.getReducerPool().put(jobId + "-" + taskId, eventQueue);
        };
        intermediateFiles = new String[rTask.getNumOfintemidateFile()];
        this.task = rTask;

    }

    @Override
    public void run() {

        ArrayList<MapperCompleteEvent> curCompleteMapperList = task.getCompleteMapperTask();

        for(MapperCompleteEvent event : curCompleteMapperList){
            //Read the partition of reducerMethod input from a mapperMethod when get notice from master
            LogSys.debug("reducerMethod " + taskId + ": current mapperMethod " + event.getmTaskId() + " at " + event.getWorkerId() + " complete event ");
            try {
                fetchIntermediateResultFromMapper(event);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        while (numOfintemidateFileLeft != 0) {
            try {
                MapperCompleteEvent event = eventQueue.take();
                //Read the partition of reducerMethod input from a mapperMethod when get notice from master
                LogSys.debug("reducerMethod " + taskId + "receive mapperMethod " + event.getmTaskId() + " at " + event.getWorkerId() + " complete event ");
                try {
                    fetchIntermediateResultFromMapper(event);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        LogSys.debug("All intermediate results were gotten by reducerMethod " + taskId);

        //got all intermediate results and start reduce phase
        Reducer reducer = null;
        try {
            Class<Reducer> reducerClass = null;
            Constructor<Reducer> constructors = null;
            reducerClass = (Class<Reducer>) Class.forName(task.getReduceMethod());
            constructors = reducerClass.getConstructor();
            reducer = constructors.newInstance();

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }


        OutputCollector out = new OutputCollector();
        ArrayList<KvPair<String, String>> inputPairs = new ArrayList<>();

        for (int i = 0; i < intermediateFiles.length; i++) {
            String file = intermediateFiles[i];
            if (file == null) {
                LogSys.err("get intermediateFiles error");
            }
            BufferedReader reader = null;
            try {

                reader = new BufferedReader(new FileReader(file));
                String line = reader.readLine();
                while (line != null) {
                    String str[] = line.split(",");
                    if (str.length != 2) {
                        LogSys.err("wrong format in intermediate result");
                    }
                    inputPairs.add(new KvPair<String, String>(str[0], str[1]));
                    line = reader.readLine();
                }

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                LogSys.err("read intermediate file error in reducerMethod " + taskId);
            } finally {
                try {
                    if (reader != null) reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        Iterator<KvPair<String, String>> values = inputPairs.iterator();
        reducer.reduce(taskId, values, out);

        //Write the final result to DFS?
        String filename = TaskTracker.getReducerOutput() + jobId + "/" + taskId;
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

        //Update reducerMethod finish info
        TaskTracker.addEvent(new TaskEvent(workerId, jobId, taskId, 1, Event.EventType.Task_Finish));
    }

    public void mapperCompleteEventNotify(MapperCompleteEvent event) throws InterruptedException {
        LogSys.log("receive mapperMethod task " + event.getmTaskId() + " at worker " + event.getWorkerId() + " complete event");
        this.eventQueue.put(event);
    }

    private void fetchIntermediateResultFromMapper(MapperCompleteEvent event) throws IOException {

        byte[] content = null;

        if (intermediateFiles[event.getmTaskId()] != null) {
            LogSys.debug("reducerMethod "+ taskId +" no need get result of mapperMethod "+event.getmTaskId()+" again");
            return;
        }

        if (event.getWorkerId() != workerId) {
            String serviceIpAddress = event.getServiceIpAddress();
            int servicePort = event.getServicePort();

            //TODO: Get service name???

            try {

                Registry registry = LocateRegistry.getRegistry(serviceIpAddress, servicePort);

                TaskTrackerInterface taskTracker = (TaskTrackerInterface) registry.lookup(TaskTracker.slaveServiceName);
                LogSys.debug("start to get intermediate result of mapperMethod task " + event.getmTaskId() +
                        " from worker " + event.getWorkerId() +" IP "+serviceIpAddress+" port " + servicePort);
                try {
                    content = taskTracker.getIntermediateResult(workerId, jobId, event.getmTaskId(), taskId);
                } catch (RemoteException e) {
                    LogSys.debug("get intermediate result of " + event.getmTaskId() + "from worker " + event.getWorkerId() + " failed");
                    e.printStackTrace();
                }

            } catch (NotBoundException e) {
                LogSys.err("register worker service fail when reducerMethod " + taskId +
                        " want to get intermediate result from worker " + event.getWorkerId());
            }
        } else {
            LogSys.debug("start to get local intermediate result of mapperMethod task " + event.getmTaskId() +
                    " from worker " + event.getWorkerId());

            content = TaskTracker.getLocalIntermediateResult(jobId, event.getmTaskId(), taskId);
            if(content == null){


                //if get intermediate result fail, repeat this operation two times. If these repeats also failed, we assume the destination is down.

                try {

                    int count = event.getCount();
                    if(count != 2) {
                        LogSys.debug("reducerMethod " + taskId + " get intermediate result of mapperMethod task " + event.getmTaskId() +
                                " from worker " + event.getWorkerId() + "fail, repeat it "+ (count+1));

                        event.addCount();
                        mapperCompleteEventNotify(event);
                    }
                } catch (InterruptedException e) {
                    LogSys.debug("reducerMethod " + taskId + " get intermediate result of mapperMethod task " + event.getmTaskId() +
                            " from worker " + event.getWorkerId() + "fail");
                    e.printStackTrace();
                }

                return;
            }
        }

        LogSys.log("reducerMethod " + taskId + " get intermediate result of mapperMethod task " + event.getmTaskId() +
                " from worker " + event.getWorkerId() + "succeed");

        //store these intermediate result into disk

        String intermediateFile = TaskTracker.partitionFilePath + jobId +
                "/" + taskId + "/" + event.getmTaskId() + "_partition_" + taskId;
        IO.writeBinaryFile(content, intermediateFile);

        numOfintemidateFileLeft--;
        intermediateFiles[event.getmTaskId()] = intermediateFile;

    }


}