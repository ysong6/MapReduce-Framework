package worker;

import Util.KvPair;
import Util.OutputCollector;

import java.util.ArrayList;
import java.util.Iterator;

public interface Reducer<K,V> {
    void reduce(Integer key, Iterator<KvPair<K,V>> values, OutputCollector out );
}
