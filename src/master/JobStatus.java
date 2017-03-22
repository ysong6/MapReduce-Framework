package master;

import java.io.Serializable;

public class JobStatus implements Serializable{
    public int jobId;
    public Job.JobState state;
    public byte[] output[];
    public long startTime;
    public long endTime;

    public JobStatus(){
        jobId = -1;
        state = Job.JobState.SUBMITTING;
    }

    public JobStatus(int id, Job.JobState s){
        jobId = id;
        state = s;
    }

}
