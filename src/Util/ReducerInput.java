package Util;

import java.io.Serializable;
import java.util.ArrayList;

//this class define a structure to store all the input chunks for a reducerMethod task.
public class ReducerInput implements Serializable{
    private int workerId;
    private String workerIp;
    private int inputKey;
    private boolean isAvailable;

//    private String fetchFunc;
    public ReducerInput(int workerId, String ip, int key){
        this.workerId = workerId;
        workerIp = ip;
        inputKey = key;
        isAvailable = false;
    }
}
