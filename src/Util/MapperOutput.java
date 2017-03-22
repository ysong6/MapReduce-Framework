package Util;

import java.util.HashMap;

public class MapperOutput<k,v> {
    private HashMap<k,v> output;

    public MapperOutput(){
        output = new HashMap<k, v>();
    }

    public void add(k key, v value){
        output.put(key, value);
    }
}
