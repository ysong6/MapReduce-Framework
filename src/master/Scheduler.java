package master;

import Util.RemoteService;
import conf.ConfigPath;
import conf.Configuration;
import log.LogSys;
import worker.TaskTracker;
import worker.TaskTrackerInterface;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

//Schedule class schedules worker resources and tasks. It is a event-driving class. It deals with worker events and task events.
//Worker events: worker adding, worker leaving
// task events: task fail, task finish
//data: worker list and task list
//schedule only maintain tasks which neet to run , the whole tasks for a job maintained in Job class and all tasks which
// run in the same worker maintained in Worker Class

public class Scheduler implements Runnable {
    private static HashMap<Integer, Worker> workers;
    private static PriorityQueue<Worker> mapperPool; //found the lightest weight worker for mapperMethod task
    private static PriorityQueue<Worker> reducerPool; //found the lightest weight worker for reducerMethod task
    private ArrayList<MTask> doMTask; // all mapperMethod tasks need to run
    private ArrayList<RTask> doRTask; // all reducerMethod tasks need to run
    private static LinkedBlockingQueue<Event> eventQueue;
    private LinkedList<Event> workQueue;

    public Scheduler() {
        Comparator<Worker> mComp = new Comparator<Worker>() {
            @Override
            public int compare(Worker o1, Worker o2) {
                return o2.getAvailableMapperSlotNum() - o1.getAvailableMapperSlotNum();
            }
        };
        mapperPool = new PriorityQueue<Worker>(mComp);

        Comparator<Worker> rComp = new Comparator<Worker>() {
            @Override
            public int compare(Worker o1, Worker o2) {
                return o2.getAvailableReducerSlotNum() - o1.getAvailableReducerSlotNum();
            }
        };
        doMTask = new ArrayList<>();
        doRTask = new ArrayList<>();
        reducerPool = new PriorityQueue<>(rComp);
        eventQueue = new LinkedBlockingQueue<>();
        workQueue = new LinkedList<>();
        workers = new HashMap<>();
    }

    public void run() {

        boolean isRun = true;

        while (isRun) {

            //step 1: transfer all events from event queue to deal queue
            Event e = null;
            try {
                e = eventQueue.take();
                while (!eventQueue.isEmpty()) {
                    workQueue.add(e);
                    e = eventQueue.take();
                }
                workQueue.add(e);

            } catch (InterruptedException e1) {
                LogSys.err("take event error for " + e.toString());
            }

            //step 2: deal with all events
            while (!workQueue.isEmpty()) {
                Event event = workQueue.poll();
                switch (e.event) {
                    case Worker_Insert:
                        workerInsert((WorkerEvent) event);
                        break;
                    case Worker_Leave:
                        workerLeave((WorkerEvent) event);
                        break;
                    case Job_Create:
                        jobCreate((JobEvent) event);
                        break;
                    case Task_Finish:
                        taskFinish((TaskEvent) event);
                        break;
                    case Task_Fail:
                        taskFail((TaskEvent) event);
                        break;
                    default:
                        LogSys.log("An unknown event is received");
                }
            }
        }
    }


    public void workerInsert(WorkerEvent e) {
        LogSys.debug("received Woker " + e.workerId + "insert event");

        int mSlot = e.getMapperSlot();
        int rSlot = e.getReducerSlot();
        String ip = e.getIpAddress();
        int port = e.getServicePort();

        // Get the Remote Object Reference from JobTracker
        RemoteService<TaskTrackerInterface> taskTracker = new RemoteService<>(e.workerId, ip, port, "TaskTracker");


        //create new worker
        Worker worker = new Worker(e.getWorkerId(), ip, port, mSlot, rSlot, taskTracker);
        workers.put(e.getWorkerId(), worker);
        mapperPool.add(worker);
        reducerPool.add(worker);

        //allocate mapperMethod task to this worker
        mTaskBatchAllocate(doMTask);

        //allocate reducerMethod task to this worker
        rTaskBatchAllocate(doRTask);
    }

    public void workerLeave(WorkerEvent e) {

        Worker worker = workers.get(e.workerId);
        LogSys.log("Worker "+e.workerId+" down \n\n");

        if (worker == null) {
            LogSys.err("worker have been already down");
        }
        workers.remove(worker.getWorkerId());

        HashMap<Integer, Job> jobs = JobTracker.getJobs();
        Iterator iter = jobs.entrySet().iterator();
        while(iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            Job job = (Job) entry.getValue();
            if(job.getCompleteMappers() != null && job.getCompleteMappers().get(e.workerId) != null){
                job.setNumOfCompleteMappers(job.getNumOfCompleteMappers() - job.getCompleteMappers().get(e.workerId).size());
                job.getCompleteMappers().remove(e.workerId);
            }
            if(job.getCompleteReducers()!= null && job.getCompleteReducers().get(e.workerId) != null){
                job.setNumOfCompleteReducers(job.getNumOfCompleteReducers() - job.getCompleteReducers().get(e.workerId).size());
                job.getCompleteReducers().remove(e.workerId);
            }
        }


        mapperPool.remove(worker);
        reducerPool.remove(worker);

        // Get all mapperMethod tasks which ever run in that worker
        ArrayList<MTask> mList = worker.getMTasks();
        ArrayList<RTask> rList = worker.getrTasks();


        //allocate these tasks if we have available resources
        mTaskBatchAllocate(mList);
        rTaskBatchAllocate(rList);

        //append all left tasks to do-task list
        if (mList.size() != 0){
            for(MTask task: mList){
                clearTaskStatus(task);
            }
            doMTask.addAll(mList);
        }
        if (rList.size() != 0){
            for(RTask task: rList){
                clearTaskStatus(task);
            }
            doRTask.addAll(rList);
        }
    }

    public void jobCreate(JobEvent e) {

        Job job = e.job;
        LogSys.log("New job " +job.getJobId() +" submit\n\n");

        job.jobPartition();

        ArrayList<RTask> rTaskList = job.getrTasks();
        ArrayList<RTask> rList = (ArrayList<RTask>) rTaskList.clone();

        rTaskBatchAllocate(rList);

        ArrayList<MTask> mTaskList = job.getmTasks();
        ArrayList<MTask> mList = (ArrayList<MTask>) mTaskList.clone();

        mTaskBatchAllocate(mList);

        if (mList.size() != 0) doMTask.addAll(mList);
        if (rList.size() != 0) doRTask.addAll(rList);

    }

    public int allocateTask(Task task) {

        if (workers.isEmpty()) {
            LogSys.log("This system has no workers now");
            return -1;
        }

        if (mapperPool.isEmpty()) {
            LogSys.err("workers table is not consistent with mapperPool");
            return -1;
        }

        if (task instanceof MTask) {

            Worker worker = mapperPool.peek();

            if (worker.hasAvailableMapperSlot()) {
                int res = worker.allocateMapperSlot((MTask) task);
                if (res == -1) {
                    LogSys.debug("has no available mapperMethod slot");

                    return -1;
                }
                mapperPool.poll();
                mapperPool.add(worker);
            } else {
                LogSys.debug("has no available mapperMethod slot");
                return -1;
            }

        } else {

            updateReducerTaskStatus((RTask) task);

            Worker worker = reducerPool.peek();

            if (worker.hasAvailableReducerSlot()) {
                reducerPool.poll();
                worker.allocateReducerSlot((RTask) task);
                reducerPool.add(worker);
            } else {
                LogSys.debug("has no available mapperMethod slot");
                return -1;
            }
        }

        return 0;
    }

    public void mTaskBatchAllocate(ArrayList<MTask> tList) {

        Iterator list = tList.iterator();
        while (list.hasNext()) {

            MTask task = (MTask) list.next();
            LogSys.debug("allocate mapperMethod task " + task.getTaskId());
            int result = allocateTask(task);
            if (result < 0) {
                break;
            }
            list.remove();
        }
    }

    public void rTaskBatchAllocate(ArrayList<RTask> tList) {

        Iterator list = tList.iterator();
        while (list.hasNext()) {
            RTask task = (RTask) list.next();
            int result = allocateTask(task);
            if (result < 0) break;
            list.remove();
        }
    }

    public void taskFinish(TaskEvent e) {

        Worker worker = workers.get(e.workerId);
        if(worker == null){
            LogSys.debug("worker "+e.workerId +"is already down");
            return;
        }

        if (e.taskType == 0) {// mapperMethod task
            LogSys.log("Mapper task " + e.taskId + " finished in worker " + e.workerId);

            //step 0: update task status
            Job job = JobTracker.getJob(e.jobId);
            MTask task = job.getMapperTask(e.taskId);
            task.updateState(Task.TaskState.SUCCEEDED);
            ArrayList<MTask> completeMTasks = job.getCompleteMappers().get(e.workerId);
            if(completeMTasks == null){
                completeMTasks = new ArrayList<>();
                job.getCompleteMappers().put(e.workerId,completeMTasks);
            }
            completeMTasks.add(task);

            //step 1: release worker resource
            worker.releaseMapperSlot();

            //step 2: assign a new task for it, if the task is there
            if (!doMTask.isEmpty()) {
                worker.allocateMapperSlot(doMTask.get(0));
                doMTask.remove(0);
            }

            //step 3: notify all reducerMethod
            notifyMapperCompleteEvent(e);

        } else {

            LogSys.log("Reducer task " + e.taskId + " finished in worker " + e.workerId);

            //step 1: update task status
            Job job = JobTracker.getJob(e.jobId);
            job.updateData4rTaskFinished(e.taskId);

            if(job.isJobFinished()){
                job.setEndTime(System.currentTimeMillis());
                collectResultsOfJob(job);
                LogSys.log("Job "+e.jobId + "finished");
                return;
            }

            //step 2: release worker resource
            worker.releaseReducerSlot();

            //step 3: assign a new task for it, if the task is there
            if (!doRTask.isEmpty()) {
                RTask rtask = doRTask.get(0);
                updateReducerTaskStatus(rtask);
                worker.allocateReducerSlot(rtask);
                doRTask.remove(0);
            }
        }
    }

    public void taskFail(TaskEvent e) {

        LogSys.debug("Task " + e.taskId + "failed in worker " + e.workerId);
        Worker worker = workers.get(e.workerId);
    }

    public void startScheduler() {

        try {
            Configuration.configParser(ConfigPath.MRConf, this);

        } catch (FileNotFoundException e) {
            LogSys.err("MRconf file is not found");
        } catch (IOException e) {
            LogSys.err(e.toString());
        }

        Thread thread = new Thread(this);
        thread.start();
    }


    public static void EventNotify(Event e) {
        try {
            eventQueue.put(e);
        } catch (InterruptedException e1) {
            LogSys.err("Event notify failed for "+ e1.toString());
        }
    }


    public void notifyMapperCompleteEvent(TaskEvent e) {
        int mWorkerId = e.workerId;
        Worker mWorker = workers.get(e.workerId);
        String serviceIP = mWorker.getIpaddress();
        int servicePort = mWorker.getServicePort();
        int mTaskId = e.taskId;
        ArrayList<RTask> list = JobTracker.getJob(e.jobId).getAllReducerTask();
        Iterator<RTask> iter = list.iterator();
        while (iter.hasNext()) {
            RTask task = iter.next();
            //should provide worker info including worker IP, id, port, mappr task id, and reducerMethod task info, including job id, reducerMethod task id
            int rTaskId = task.taskId;
            if(task.status == Task.TaskState.WAITING) {
                continue;
            }

            task.worker.notifyMapperComplete(mWorkerId, serviceIP, servicePort, e.jobId, mTaskId, rTaskId);
        }
    }

    public void updateReducerTaskStatus(RTask task) {

        ArrayList<MapperCompleteEvent> completeMappers = new ArrayList<>();

        Iterator iter = JobTracker.getJob(task.jobId).getCompleteMappers().entrySet().iterator();

        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            ArrayList<MTask> list = (ArrayList<MTask>) entry.getValue();
            completeMappers.addAll(generateMapperCompleteEvent(list,task));
        }

        task.setCompleteMapperTask(completeMappers);
    }

    public ArrayList<MapperCompleteEvent> generateMapperCompleteEvent(ArrayList<MTask> list, RTask rTask){

        ArrayList<MapperCompleteEvent> events = new ArrayList<>();
        if(list == null){
            return events;
        }

        for(MTask task: list) {
            int mWorkerId = task.getWorkerId();
            Worker mWorker = workers.get(mWorkerId);
            String serviceIP = mWorker.getIpaddress();
            int servicePort = mWorker.getServicePort();
            int mTaskId = task.taskId;
            int jobId = task.getJobId();

            MapperCompleteEvent e = new MapperCompleteEvent(mWorkerId, serviceIP, servicePort, jobId, mTaskId, rTask.taskId);
            events.add(e);
        }

        return events;
    }

    public void clearTaskStatus(Task task){
        task.setStatus(Task.TaskState.WAITING);
        task.setWorer(null);
        task.setWorkerId(-1);
        task.setStartTime(0);
        if(task instanceof RTask){
            ArrayList<MapperCompleteEvent> completeMappers = new ArrayList<>();
            ((RTask) task).setCompleteMapperTask(completeMappers);
        }
    }

    public void collectResultsOfJob(Job job){
        Collector collector = new Collector(job);
        Thread t = new Thread(collector);
        t.start();
    }

    public static void clearTaskInfo(int jobId){

        Iterator iter = workers.entrySet().iterator();

        while(iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            Worker worker = (Worker) entry.getValue();
            worker.clearTasks();
            mapperPool.remove(worker);
            mapperPool.add(worker);
            reducerPool.remove(worker);
            reducerPool.add(worker);
        }
    }
}
