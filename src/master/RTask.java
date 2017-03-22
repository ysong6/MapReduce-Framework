package master;

import Util.MapperInput;
import Util.ReducerInput;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;


public class RTask extends Task {
    private ReducerInput[] inputs;
    private String output;
    private String reduceMethod;
    private int numOfintemidateFile;
    private ArrayList<MapperCompleteEvent> curCompleteMapperTask;

    public RTask(int jobId, int taskId, int numOfintemidateFile, String out, String reduceMethod, ArrayList<MapperCompleteEvent> completeMapperTask) {
        this.jobId = jobId;
        this.taskId = taskId;
        inputs = new ReducerInput[numOfintemidateFile];
        output = out;

        status = TaskState.WAITING;
        startTime = System.currentTimeMillis();
        this.numOfintemidateFile = numOfintemidateFile;
        this.reduceMethod = reduceMethod;
        this.curCompleteMapperTask = completeMapperTask;
    }

    public ReducerInput[] getInputs() {
        return inputs;
    }

    public String getOutput() {
        return output;
    }

    public String getReduceMethod() {
        return reduceMethod;
    }

    public int getNumOfintemidateFile() {
        return numOfintemidateFile;
    }

    public void setWorkerId(int workerId) {
        this.workerId = workerId;
    }

    public int getWorkerId() {
        return workerId;
    }

    public void setCompleteMapperTask(ArrayList<MapperCompleteEvent> completeMapperTask) {
        this.curCompleteMapperTask = completeMapperTask;
    }

    public ArrayList<MapperCompleteEvent> getCompleteMapperTask() {
        return curCompleteMapperTask;
    }

    public void setCurCompleteMapperTask(ArrayList<MapperCompleteEvent> curCompleteMapperTask) {
        this.curCompleteMapperTask = curCompleteMapperTask;
    }
}
