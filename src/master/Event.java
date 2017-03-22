package master;

import java.io.Serializable;

public class Event<v> implements Serializable{

    public EventType event;
    public long time;

    public static enum EventType{ Worker_Insert, Worker_Leave, Task_Finish, Task_Fail, Job_Create, MAPPER_COMPLETE}
    public static enum FailType{FileNotFound, IOFail, LackOfResource, Unknown, TimeOut}

    public Event(){
        time = System.currentTimeMillis();
    }

}
