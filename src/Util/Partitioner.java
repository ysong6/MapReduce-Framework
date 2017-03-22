package Util;

//partition mapperMethod result into reducerMethod input
public class Partitioner {
    public static int partition(KvPair kvPair, int numPartitions){
        return kvPair.hashCode()%numPartitions;
    }
}
