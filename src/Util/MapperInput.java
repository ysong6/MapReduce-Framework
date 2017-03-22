package Util;

import java.io.Serializable;

public class MapperInput implements Serializable {
    private String inputFile;
    private int offset;
    private int len;
    private UserMethod partitionClass;


    public MapperInput(String in, int offset, int len, UserMethod partitionClass){
        inputFile = in;
        this.offset = offset;
        this.len = len;
        this.partitionClass = partitionClass;
    }

    public String getInputFile() {
        return inputFile;
    }

    public int getOffset() {
        return offset;
    }

    public int getLen() {
        return len;
    }

    public UserMethod getPartitionClass() {
        return partitionClass;
    }

}
