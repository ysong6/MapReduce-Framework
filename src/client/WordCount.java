package client;

import Util.Partition4TextFile;
import Util.UserMethod;
import conf.ConfigPath;
import conf.Configuration;
import conf.JobConf;
import log.LogSys;
import master.Job;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.RemoteException;

public class WordCount {
    private String mapperPath;
    private String reducerPath;
    private String inputFile;
    private String outputFile;

    public static void main(String[] args) throws RemoteException {

        JobConf wordCount = new JobConf();

        try {
            Configuration.configParser(ConfigPath.userConf, wordCount);
            LogSys.setLogSys(wordCount.jobClientIp,"Job Client");

        } catch (FileNotFoundException e) {
            System.out.println("job configuration file was not found");
        } catch (IOException e) {
            System.out.println(e.toString());
        }

        wordCount.mapperMethod = new UserMethod(WordCountMapper.class, wordCount.mapperMethodPath);
        wordCount.reducerMethod = new UserMethod(WordCountReducer.class, wordCount.reducerMethodPath);
        wordCount.partitionMethod = new UserMethod(Partition4TextFile.class, wordCount.partitionMethodPath);
        Job job = new Job(wordCount);

        try {
            job.run();
        } catch (IOException e) {
			e.printStackTrace();
        }
    }
}
