package master;


import log.LogSys;
import worker.TaskTrackerInterface;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class Collector implements Runnable {
    private Job job;

    public Collector(Job job) {
        this.job = job;
    }

    public void run(){


        ArrayList<RTask> rTaskList = job.getAllReducerTask();
        byte[] result[] = new byte[rTaskList.size()][];

        for(int i = 0; i < rTaskList.size(); i++){

            RTask task = rTaskList.get(i);
            TaskTrackerInterface taskTracker = (TaskTrackerInterface) task.worker.remoteService.getRemoteService();
            try {
                result[i] = taskTracker.getResultOfReducer(task.getWorkerId(),task.jobId,task.taskId);
            } catch (IOException e) {
                LogSys.err("get result of reducerMethod "+task.taskId+" failed");
                e.printStackTrace();
            }
        }

        job.updateJobCompeletInfo(result);
    }
}
