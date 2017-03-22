package master;

import Util.KvPair;
import Util.MapperInput;
import Util.UserMethod;

import java.util.ArrayList;

//mapperMethod task including context and process info
public class MTask extends Task{
    private MapperInput input;
    private String mapperName;
    private int partitionFactor; //it is partition base number
    private ArrayList<KvPair>[] output;
    private UserMethod partition;
    private UserMethod mapper;


    public MTask(int jId, int tId, MapperInput input, int rNum, UserMethod mapper, UserMethod partition){
        jobId = jId;
        taskId = tId;
        this.input = input;
        partitionFactor = rNum;
        status = TaskState.RUNNING;
        output = new ArrayList[rNum];
        for(int i = 0; i < rNum; i++){
            output[i]= new ArrayList<KvPair>();
        }
        startTime = System.currentTimeMillis();
        this.mapper = mapper;
        this.partition = partition;
        mapperName = mapper.getUserClass().getName();
        this.workerId = -1;
    }


    public int getPartitionFactor(){
        return partitionFactor;
    }

    public void outputCollect(int key, KvPair pair){
        output[key].add(pair);
    }


    public String getMapperName() {
        return mapperName;
    }
    public MapperInput getInput() {
        return input;
    }

    public ArrayList<KvPair>[] getOutput() {
        return output;
    }

    public UserMethod getPartition() {
        return partition;
    }

    public UserMethod getMapper() {
        return mapper;
    }
}