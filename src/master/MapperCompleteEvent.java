package master;

public class MapperCompleteEvent extends Event{
    private int workerId;
    private String serviceIpAddress;
    private int servicePort;
    private int jobId;
    private int rTaskId;
    private int mTaskId;
    private int count;

    public MapperCompleteEvent(int workerId, String serviceIpAddress, int servicePort, int jobId, int mTaskId, int rTaskId){
        this.jobId = jobId;
        this.mTaskId = mTaskId;
        this.rTaskId = rTaskId;
        this.serviceIpAddress = serviceIpAddress;
        this.servicePort = servicePort;
        this.workerId = workerId;
        this.count = 0;
    }

    public int getWorkerId() {
        return workerId;
    }

    public String getServiceIpAddress() {
        return serviceIpAddress;
    }

    public int getServicePort() {
        return servicePort;
    }

    public int getJobId() {
        return jobId;
    }

    public int getrTaskId() {
        return rTaskId;
    }

    public int getmTaskId() {
        return mTaskId;
    }

    public int getCount() {
        return count;
    }
    public void addCount(){
        count++;
    }
}
