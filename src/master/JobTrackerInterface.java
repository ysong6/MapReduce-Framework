package master;

import Util.KvPair;

import java.io.IOException;
import java.rmi.Remote;

public interface JobTrackerInterface extends Remote{
    public JobStatus submit(Job job, byte[] mapper, byte[] reducer) throws IOException;
    public JobStatus getJobStatus(int jobId) throws IOException;
}
