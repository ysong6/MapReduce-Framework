package master;

import java.io.Serializable;

public class WorkerEvent extends Event{
    public int workerId;
    public String ipAddress;
    public int servicePort;
    public int mapperSlot;
    public int reducerSlot;
    public master.TaskEvent.FailType failType;

    public WorkerEvent(master.TaskEvent.EventType e, int wId, master.TaskEvent.FailType f) {
        super.event = e;
        this.workerId = wId;
        this.failType = f;
    }

    public WorkerEvent(master.TaskEvent.EventType e, int wId, String ip, int servicePort, int mapperSlot, int reducerSlot) {
        super.event = e;
        this.workerId = wId;
        this.ipAddress = ip;
        this.servicePort = servicePort;
        this.mapperSlot = mapperSlot;
        this.reducerSlot = reducerSlot;
    }

    public int getWorkerId(){
        return workerId;
    }

    public int getMapperSlot(){
        return mapperSlot;
    }

    public int getReducerSlot(){
        return reducerSlot;
    }

    public String getIpAddress(){
        return ipAddress;
    }

    public int getServicePort(){
        return servicePort;
    }
}
