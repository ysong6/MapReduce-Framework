package Util;

import master.Worker;

import java.util.*;

public class OutputCollector{

    public PriorityQueue<KvPair<String,String>> outputCollector;


    public OutputCollector(){
        Comparator<KvPair<String,String>> mComp = new Comparator<KvPair<String,String>>() {
            @Override
            public int compare(KvPair<String,String> pair1, KvPair<String,String> pair2) {
                return pair1.getKey().compareTo(pair2.getKey());
            }
        };
        outputCollector = new PriorityQueue<KvPair<String,String>>(mComp);
    }

    /**
     * This method is used to add key and value pair into the collector
     *
     * @param key   Object
     * @param value Object
     */

    public void add(String key, String value) {
        KvPair pair = new KvPair(key,value);
        outputCollector.add(pair);
    }
}
