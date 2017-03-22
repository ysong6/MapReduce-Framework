package Util;

import log.LogSys;

import java.io.*;
import java.util.ArrayList;

/**
 * Partition text file into key-value pairs(key is number of line, value is line) according to offset and length.
 */
public class Partition4TextFile implements Partition {

    public ArrayList<KvPair<Integer,String>> partition(String filename, int off, int len) throws IOException {

        ArrayList<KvPair<Integer,String>> res = new ArrayList<>();
        byte buffer[] = new byte[len];
        String part = null;

        try {

            RandomAccessFile randomFile = new RandomAccessFile(filename, "r");
            randomFile.seek(off);
            int rLen = randomFile.read(buffer);
            String strRead = new String(buffer);

            int offset = off;
            String str = null;

            if((strRead.charAt(0) != ' '|| strRead.charAt(0) != '\n') && offset != 0) { // find out the start of this word
                StringBuilder sb = new StringBuilder();
                while (true) {
                    int pre = offset > 100 ? 100:offset;
                    byte head[] = new byte[pre];
                    randomFile.seek(offset-pre);
                    int rLen1 = randomFile.read(head);
                    int index = pre-1;

                    String preStr = new String(head);

                    while(index >= 0 && preStr.charAt(index) != ' ' &&  preStr.charAt(index) != '\n'){
                        sb.insert(0,preStr.charAt(index--));
                    }

                    if(index >= 0 || offset < 100) {
                        str = new String(sb.toString() + strRead);
                        break;
                    }
                    offset -= pre;
                }
            }

            if(str == null) str = strRead;

            int end = str.length()-1;
            while(end >= 0 && str.charAt(end) != ' ' && str.charAt(end) != '\n') end--;
            if(end < 0) end = 0;
            part = str.substring(0,end);

        }catch (FileNotFoundException e) {
            throw new IOException(e.toString());
        }

        if(part.length() == 0) return res;

        int index = 0;
        String[] lines = part.trim().split("\n");
        for (String line : lines) {
            KvPair<Integer,String> pair = new KvPair<>(index++,line);
            res.add(pair);
        }

        return res;
    }
}
