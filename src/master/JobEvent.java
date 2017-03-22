package master;

public class JobEvent extends Event {
    public Job job;

    public JobEvent(master.TaskEvent.EventType e, Job o) {
        super.event = e;
        this.job = o;
    }

}
