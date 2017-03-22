package worker;

import Util.*;
import log.LogSys;
import master.Event;
import master.MTask;
import master.TaskEvent;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * This class is to run one thread for mapperMethod task.
 */
public class MapperThread implements Runnable {
    private MTask task;
    private int workerId;
    private int jobId;
    private int taskId;
    private Mapper mapper;
    //TODO: Hardcoded?

    public MapperThread(MTask task) {
        this.task = task;
        this.workerId = task.getWorkerId();
        this.jobId = task.getJobId();
        this.taskId = task.getTaskId();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {

        /* 1. Get the mapperMethod class */
        Class<Mapper> mapClass = null;


        try {
            mapClass = (Class<Mapper>) Class.forName(task.getMapperName());
            Constructor<Mapper> constructors = mapClass.getConstructor();
            mapper = constructors.newInstance();

            /* 2. Read and Format input data and fill the result into mapperOutput*/
            //read data from IO and process to KV pairs.

            Class<Partition> partition = (Class<Partition>) task.getPartition().getUserClass();
            Constructor<Partition> constructor = partition.getConstructor();
            Partition partionMethod = constructor.newInstance();

            ArrayList<KvPair<Integer, String>> inputPair = new ArrayList<>();
            MapperInput input = task.getInput();

            try {
                inputPair = partionMethod.partition(input.getInputFile(), input.getOffset(), input.getLen());
            } catch (IOException e) {
                e.printStackTrace();
            }

            for (KvPair<Integer, String> pair : inputPair) {
                mapper.map(pair.getKey(), pair.getValue(), task);
            }

        /* 4. write the output */
            writeIntermediateResult(task);

            LogSys.log("task " + task.getTaskId() + "finished");

        /* 5. Notify the master */
            TaskTracker.addEvent(new TaskEvent(workerId, jobId, taskId, 0, Event.EventType.Task_Finish));

        } catch (ClassNotFoundException | NoSuchMethodException |
                IllegalAccessException | InstantiationException | InvocationTargetException e) {
            e.printStackTrace();
            //TODO: handle mapperMethod failure.
            TaskTracker.addEvent(new TaskEvent(workerId, jobId, taskId, 0, Event.EventType.Task_Fail));
            System.err.println("Mapper fails.");
            System.exit(-1);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

    }


    public void writeIntermediateResult(MTask task) throws IOException {

        String intermediateFiles[] = new String[task.getPartitionFactor()];

        for (int i = 0; i < task.getPartitionFactor(); i++) {
            String filename = TaskTracker.intermediateFilePath + jobId + "/" + taskId + "_intermediatePartition_" + i + ".txt";
            intermediateFiles[i]= filename;
            ArrayList<KvPair>[] out = task.getOutput();

            int index = filename.length() - 1;
            while (index >= 0 && filename.charAt(index) != '/') {
                index--;
            }
            String dir = filename.substring(0, index);

            File fileDir = new File(dir);
            if (!fileDir.exists()) {
                System.out.println("create dir: " + dir);
                fileDir.mkdirs();
            }

            File file = new File(filename);
            if (!file.exists()) {
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    throw new IOException(e.toString());
                }
            }

            FileWriter fileWriter = new FileWriter(file);

            for (KvPair<String, Integer> pair : out[i]) {
                fileWriter.write(pair.getKey() + "," + pair.getValue() + "\n");
            }

            fileWriter.close();
        }

        TaskTracker.addIntermediateFiles(jobId,taskId,intermediateFiles);
    }

}
