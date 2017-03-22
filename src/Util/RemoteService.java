package Util;

import log.LogSys;
import master.Task;
import worker.TaskTrackerInterface;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class RemoteService<v> {
    private int workerId;
    private Registry registry;
    private TaskTrackerInterface remoteService;
    private int type;

    public RemoteService(int workerId, String ipAddress, int servicePort, String serviceName) {
        try{
            registry = LocateRegistry.getRegistry(ipAddress, servicePort);
            remoteService = (TaskTrackerInterface) registry.lookup("TaskTracker");
        } catch (NotBoundException |IOException e1) {
            LogSys.err("Failure happened when looking up the service!");
            e1.printStackTrace();
        }
    }

    public void threadStart(RemoteCall rCall){
        Thread thread = new Thread(rCall);
        thread.start();
    }


    public TaskTrackerInterface getRemoteService(){
        return remoteService;
    }

    public int getWorkerId() {
        return workerId;
    }
}
