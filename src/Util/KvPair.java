package Util;

public class KvPair<k,v> {
    private k key;
    private v value;

    public KvPair(k key, v value) {
        this.key = key;
        this.value = value;
    }
    @Override
    public int hashCode(){
        return key.hashCode();
    }
    public k getKey(){
        return key;
    }
    public v getValue(){
        return value;
    }

}
