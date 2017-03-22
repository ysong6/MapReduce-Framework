package master;

public class TaskEvent extends Event {
    public int workerId;
    public int jobId;
    public int taskId;
    public int taskType; // set 0 equal to Mapper, set 1 equal to Reducer
    public master.TaskEvent.FailType failType;

    public TaskEvent(int wId, int jId, int tId, int type, master.TaskEvent.EventType e, master.TaskEvent.FailType f) {
        super.event = e;
        this.jobId = jId;
        this.workerId = wId;
        this.taskId = tId;
        this.taskType = type;
        this.failType = f;
    }

    public TaskEvent(int wId, int jId, int tId, int type, master.TaskEvent.EventType e) {
        super.event = e;
        this.jobId = jId;
        this.workerId = wId;
        this.taskId = tId;
        this.taskType = type;
    }
}