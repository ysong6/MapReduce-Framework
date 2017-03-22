package Util;

import java.io.IOException;
import java.util.ArrayList;

public interface Partition {
    public ArrayList<KvPair<Integer,String>> partition(String filename, int off, int len) throws IOException;
}
