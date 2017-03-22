package master;

import java.io.Serializable;

public class Task  implements Serializable {
    protected int jobId;
    protected transient Worker worker;
    protected int taskId;
    protected transient TaskState status;
    protected int workerId;
    protected long startTime;
    protected long endTime;

    public static enum TaskState {WAITING, RUNNING, SUCCEEDED, FAILED, UNASSIGNED, KILLED,
        COMMIT_PENDING, FAILED_UNCLEAN, KILLED_UNCLEAN}



    public void updateState(TaskState state){
        status = state;
    }

    //if we need store this info here
    public void bindWorker(Worker worker){
        this.worker = worker;
    }


    public void notifyMapperComplete(){

    }

    public int getJobId() {
        return jobId;
    }

    public Worker getWorker() {
        return worker;
    }
    public void setWorer(Worker worker){
        this.worker = worker;
    }
    public int getTaskId() {
        return taskId;
    }

    public TaskState getStatus() {
        return status;
    }

    public void setStatus(TaskState status) {
        this.status = status;
    }

    public void setWorkerId(int workerId) {
        this.workerId = workerId;
    }

    public int getWorkerId() {
        return workerId;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }
}
