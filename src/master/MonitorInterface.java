package master;


import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;

public interface MonitorInterface extends Remote {

    public int heartBeat(int workerId, ArrayList<TaskEvent> taskEvent) throws IOException;

    public int workerRegister(String ipaddress, int servicePort, int mapperSlot, int reducerSlot) throws IOException;

    public byte[] getReducerClass(int workerId, int jobId) throws IOException;

    public byte[] getMapperClass(int workerId, int jobId) throws IOException;

}
