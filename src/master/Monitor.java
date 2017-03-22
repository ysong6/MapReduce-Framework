package master;

import Util.IO;
import conf.ConfigPath;
import conf.Configuration;
import log.LogSys;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;

public class Monitor extends UnicastRemoteObject implements MonitorInterface{
    private int monitorServicePort;
    private static volatile HashMap<Integer,HeartbeatStatus> aliveWorkers; //<workerId, HeartbeatClick>
    private Integer nextWorkerId;
    private int heartBeatInterval;

    public Monitor() throws RemoteException {
        aliveWorkers = new HashMap<>();
        nextWorkerId = 0;
    }

    public void startMonitor() throws IOException{
        try {
            Configuration.configParser(ConfigPath.MRConf, this);

            unexportObject(this, false);

            String url = "rmi://"+JobTracker.masterServiceIpAddress+":"+monitorServicePort+"/Monitor";
            Registry registry = LocateRegistry.createRegistry(monitorServicePort);
            Naming.rebind(url, this);
            startHeartBeatTimer();

        } catch (FileNotFoundException e) {
            LogSys.err("MRconf file is not found");
        } catch (IOException e) {
            LogSys.err(e.toString());
        }
//        } catch (AlreadyBoundException e) {
//            e.printStackTrace();
//        }
    }

    public void startHeartBeatTimer() {

        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                checkHeartBeat();
            }
        };
        new Timer().scheduleAtFixedRate(timerTask, 0, heartBeatInterval);
    }

    public int checkHeartBeat(){
        ArrayList<Integer> del = new ArrayList<>();

        for(Integer id: aliveWorkers.keySet()){
            HeartbeatStatus state = aliveWorkers.get(id);
            synchronized (state) { //divide this process into two phase, in order to decrease the whole lock time
                if (state.isReceive) {
                    state.isReceive = false;
                    state.count = 0;
                } else if (state.isReceive == false && state.count < 5) {
                    state.count++;
                } else if (state.count == 5) { //didn't receive heartbeat 6 times means worker must be down
                    del.add(id);
                    WorkerEvent e = new WorkerEvent(TaskEvent.EventType.Worker_Leave, id, Event.FailType.TimeOut);
                    Scheduler.EventNotify(e);
                }
            }
        }

        if(del.size() != 0) {
            synchronized (aliveWorkers) {
                for(Integer n : del) {
                    LogSys.debug("Worker "+ n + " down");
                    aliveWorkers.remove(n);
                }
            }
        }

        return 0;
    }

    public int workerRegister(String ipaddress, int servicePort, int mapperSlot, int reducerSlot) throws IOException{
        int workerId = -1;

        synchronized(nextWorkerId){
            workerId = nextWorkerId;
            nextWorkerId++;
        }

        WorkerEvent e = new WorkerEvent(TaskEvent.EventType.Worker_Insert, workerId, ipaddress, servicePort, mapperSlot, reducerSlot);
        Scheduler.EventNotify(e);

        synchronized (aliveWorkers) {
            aliveWorkers.put(workerId, new HeartbeatStatus());
        }

        LogSys.log("Worker " + workerId +" ("+ipaddress+")" + " registered");


        return workerId;
    }

    public int heartBeat(int workerId, ArrayList<TaskEvent> taskEvent) throws IOException{

        boolean isWorkerDown = false;

//        LogSys.debug("Received worker " + workerId +"heartbeat");

        synchronized (aliveWorkers) {
            HeartbeatStatus seq = aliveWorkers.get(workerId);
            if (seq != null) {
                seq.isReceive = true;
            }else isWorkerDown = true;
        }

        for(TaskEvent e : taskEvent){
            LogSys.debug("Received task event: " + e.taskId + e.taskType);

            Scheduler.EventNotify(e);
        }

        if(isWorkerDown){//Master thought worker was down, then receives the heartbeat from that worker. This version only log it.
            LogSys.debug("worker " + workerId + "does not exist when receive heartbeat from it");
        }

        return 0;
    }


    private class HeartbeatStatus{
        private volatile boolean isReceive;
        private volatile int count;

        private HeartbeatStatus(){
            isReceive = true;
            count = -2;
        }
    }

    public static int getAliveWorkerNum(){
        return aliveWorkers.size();
    }


    public byte[] getReducerClass(int workerId, int jobId){

        LogSys.debug("worker "+workerId + "get reducerMethod method for job "+jobId);
        String file = JobTracker.getJob(jobId).getMapperPath();
        File f = new File(file);
        byte [] mapper = new byte[(int)f.length()];

        try {
            IO.writeBinaryFile(mapper, file);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return mapper;
    }


    public byte[] getMapperClass(int workerId, int jobId) throws IOException{

        LogSys.debug("worker "+workerId + "get reducerMethod method for job "+jobId);

        String file = JobTracker.getJob(jobId).getReducerPath();
        File f = new File(file);
        byte [] reducerMethord = new byte[(int)f.length()];

        try {
            IO.writeBinaryFile(reducerMethord, file);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return reducerMethord;

    }



}
