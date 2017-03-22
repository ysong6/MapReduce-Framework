package conf;

import Util.UserMethod;

public class JobConf {
    public String serviceIp;
    public int servicePort;
    public String inputFile;
    public UserMethod partitionMethod;
    public UserMethod mapperMethod;
    public UserMethod reducerMethod;
    public int partitionSize;
    public String outputFile;
    public String mapperMethodPath;
    public String reducerMethodPath;
    public String partitionMethodPath;
    public String jobClientIp;
}
