package client;

import Util.KvPair;
import Util.OutputCollector;
import worker.Reducer;
import master.RTask;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class WordCountReducer  implements Reducer<String,String>{
    private HashMap<String, Integer> wordsMap;

    public WordCountReducer(){
        wordsMap = new HashMap<>();
    }

    @Override
    public void reduce(Integer key, Iterator<KvPair<String,String>> values, OutputCollector out){

        while(values.hasNext()){
            KvPair<String,String> pair = (KvPair<String,String>) values.next();

            String word = pair.getKey();
            int count = Integer.valueOf(pair.getValue());
            if(wordsMap.containsKey(word)){
                int curCount = wordsMap.get(word)+count;
                wordsMap.put(word, curCount);
            }else{
                wordsMap.put(word,1);
            }
        }
        for(String word : wordsMap.keySet()) {
            KvPair<String,String> pair = new KvPair<>(word,""+wordsMap.get(word));
            out.outputCollector.add(pair);
        }
    }

}
