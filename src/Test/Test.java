package Test;

import Util.*;

import java.io.IOException;
import java.util.ArrayList;

public class Test {

    public static void main(String args[]) {

//        String mapperPath = "./out/production/project/client/WordCountMapper.class";
//        String reducerPath = "./out/production/project/client/WordCountReducer.class";
//        String inputFile = "./src/client/test.txt";
//
//        UserMethod mapperMethod = new UserMethod(WordCountMapper.class, mapperPath);
//        UserMethod reducerMethod = new UserMethod(WordCountReducer.class, reducerPath);
//        UserMethod partition = new UserMethod(Partition4TextFile.class, null);
//
//        Job job = new Job("127.0.0.1", 9000, inputFile, partition, mapperMethod, reducerMethod, 10);
//
//        job.jobPartition();

        ArrayList<KvPair<Integer, String>> inputPair = new ArrayList<>();
        Partition4TextFile p = new Partition4TextFile();

        //            inputPair = p.partition("./src/client/test.txt", 7, 19);
        try {
            ArrayList<String> lines = IO.readTextFileByLine("./src/client/test.txt");
            System.out.println(lines.get(0));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
