package worker;

import master.MapperCompleteEvent;
import master.Task;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * This class is the interface for TaskTracker.
 */

public interface TaskTrackerInterface extends Remote {
    /**
     * This method is used to start a mapperMethod or reducerMethod thread.
     */
    void allocateTask(Task task) throws RemoteException;

    /**
     * This method is used to get the partition from a Mapper.
     */
    byte[] getIntermediateResult(int rWorkerId, int jobId, int dstTaskId, int selfTaskId) throws IOException;
    byte[] getResultOfReducer(int rWorkerId, int jobId, int rTaskId) throws IOException;

    /**
     * This method is used to notify the reducerMethod to get partition from a specific mapperMethod.
     */
    void mapperCompleteEventNotify(MapperCompleteEvent event) throws RemoteException;

}
