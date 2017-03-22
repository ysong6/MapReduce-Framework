package client;

import Util.KvPair;
import master.MTask;
import worker.Mapper;

public class WordCountMapper implements Mapper {


    @Override
    public void map(int key, String value, MTask task) {

        String words[] = value.split(" ");
        int partitionFactor = task.getPartitionFactor();
        for(String word : words){
            word  = word.replaceAll("[\\pP.,‘’“”]", "");
            if(word.length() == 0) continue;
            int hashcode = (word.toLowerCase().charAt(0)-'a')% partitionFactor;
            if(hashcode < 0){
                hashcode = Math.abs(hashcode) % partitionFactor;
            }

            KvPair<String, Integer> pair = new KvPair<>(word,1);

            task.outputCollect(hashcode, pair);
        }
    }

}
