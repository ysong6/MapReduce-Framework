package Util;

import log.LogSys;
import master.MapperCompleteEvent;
import master.Task;
import worker.TaskTracker;
import worker.TaskTrackerInterface;

import java.rmi.RemoteException;

public class RemoteCall implements Runnable{
    private RemoteService remoteService;
    private RemoteCallContext context;

    public RemoteCall(RemoteService service, RemoteCallContext context){
        this.remoteService = service;
        this.context = context;
    }

    public void run() {
        switch(context.type){
            case ALLOCATE_TASK:
                try {
                    allocateTask2Worker((Task) context.context);
                } catch (RemoteException e) {
                    LogSys.err("allocate task to worker "+ ((Task) context.context).getWorker().getWorkerId() + " fail");
                    e.printStackTrace();
                }
                break;
            case MTASK_COMPLETE:
                try {
                    mTaskCompleteNotify((MapperCompleteEvent) context.context);
                } catch (RemoteException e) {
                    MapperCompleteEvent event = (MapperCompleteEvent) context.context;
                    LogSys.err("Mapper "+ event.getmTaskId()+ "complete notify worker"+  remoteService.getWorkerId() + "failed ");
                }
            default:
                break;
        }
    }

    public void allocateTask2Worker(Task task) throws RemoteException {
        TaskTrackerInterface taskTracker = (TaskTrackerInterface) remoteService.getRemoteService();
        taskTracker.allocateTask(task);
    }

    public void mTaskCompleteNotify(MapperCompleteEvent event) throws RemoteException {
        TaskTrackerInterface taskTracker = (TaskTrackerInterface) remoteService.getRemoteService();
        taskTracker.mapperCompleteEventNotify(event);

    }

}
