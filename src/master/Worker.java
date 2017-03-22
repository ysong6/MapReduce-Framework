package master;


import Util.RemoteCall;
import Util.RemoteCallContext;
import Util.RemoteService;
import log.LogSys;

import java.util.ArrayList;

public class Worker {
    private int workerId;
    private boolean isAlive;
    private String ipaddress;
    private int servicePort;
    private int mapperSlot;
    private int reducerSlot;
    private int availableMapperSlot;
    private int availableReducerSlot;
    private ArrayList<MTask> mTasks;
    private ArrayList<RTask> rTasks;
    public RemoteService remoteService;

    public Worker(int id, String ip, int servicePort, int mSlot, int rSlot, RemoteService remoteService) {
        workerId = id;
        isAlive = true;
        ipaddress = ip;
        this.servicePort = servicePort;
        mapperSlot = mSlot;
        reducerSlot = rSlot;
        availableMapperSlot = mSlot;
        availableReducerSlot = rSlot;
        mTasks = new ArrayList<>();
        rTasks = new ArrayList<>();
        this.remoteService = remoteService;
    }

    public boolean hasAvailableMapperSlot() {
        if (availableMapperSlot == 0) return false;
        return true;
    }

    public boolean hasAvailableReducerSlot() {
        if (availableReducerSlot == 0) return false;
        return true;
    }

    public int getAvailableMapperSlotNum() {
        return availableMapperSlot;
    }

    public int getAvailableReducerSlotNum() {
        return availableReducerSlot;
    }

    public int allocateMapperSlot(MTask mTask) {
        if (availableMapperSlot == 0){
            LogSys.log("allocate mapperMethod task" + mTask.getTaskId() +" to worker" +this.workerId+"fail because of no available slot");

            return -1;
        }
        mTask.setWorkerId(workerId);
        mTask.setWorer(this);
        allocateTask2Worker(mTask);
        mTask.updateState(MTask.TaskState.RUNNING);
        availableMapperSlot--;
        mTasks.add(mTask);
        return availableMapperSlot;
    }

    public int allocateReducerSlot(RTask rTask) {

        if (availableReducerSlot == 0) return -1;
        rTask.setWorkerId(workerId);
        rTask.setWorer(this);
        rTask.setStatus(Task.TaskState.RUNNING);
        allocateTask2Worker(rTask);
        availableReducerSlot--;

        rTask.updateState(MTask.TaskState.RUNNING);
        rTasks.add(rTask);
        return availableReducerSlot;
    }

    public void allocateTask2Worker(Task task) {
        RemoteCall rpc = null;

        if (task instanceof MTask) {
            LogSys.log("allocate mapperMethod task" + task.getTaskId() +"to worker" +this.workerId);

            RemoteCallContext<MTask> context = new RemoteCallContext<MTask>(RemoteCallContext.RemoteCallType.ALLOCATE_TASK, (MTask) task);
            rpc = new RemoteCall(remoteService, context);

        } else {
            LogSys.log("allocate reducerMethod task" + task.getTaskId() +"to worker" +this.workerId);

            RemoteCallContext<RTask> context = new RemoteCallContext<RTask>(RemoteCallContext.RemoteCallType.ALLOCATE_TASK, (RTask) task);
            rpc = new RemoteCall(remoteService, context);
        }
        remoteService.threadStart(rpc);
    }

    public void notifyMapperComplete(int mWorkerId, String ipAddress, int servicePort, int jobId, int mTaskId, int rTaskId) {
        RemoteCall rpc = null;
        MapperCompleteEvent e = new MapperCompleteEvent(mWorkerId, ipAddress, servicePort, jobId, mTaskId, rTaskId);
        RemoteCallContext<MapperCompleteEvent> context = new RemoteCallContext<>(RemoteCallContext.RemoteCallType.MTASK_COMPLETE,e );
        rpc = new RemoteCall(remoteService, context);
        remoteService.threadStart(rpc);
    }

    public void addMTask(MTask task) {
        mTasks.add(task);
    }

    public ArrayList<MTask> getMTasks() {
        return mTasks;
    }

    public void addRTask(RTask task) {
        rTasks.add(task);
    }

    public ArrayList<RTask> getrTasks() {
        return rTasks;
    }

    public void releaseMapperSlot() {
        availableMapperSlot++;
    }

    public void releaseReducerSlot() {
        availableReducerSlot++;
    }
    public int getWorkerId(){
        return workerId;
    }
    public String getIpaddress(){
        return ipaddress;
    }
    public int getServicePort(){
        return servicePort;
    }
//
//    {
//        TaskThread mapTask = new TaskThread(node, jobID, jobConf,
//                nodeToChunks.get(node), true, 0, null, 0,
//                taskTrackerRegPort, taskTrackServiceName);
//        executor.execute(mapTask);
//    }

    public void clearTasks(){
        mTasks = new ArrayList<>();
        rTasks = new ArrayList<>();
        availableMapperSlot = mapperSlot;
        availableReducerSlot = reducerSlot;
    }
}
