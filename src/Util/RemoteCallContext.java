package Util;

public class RemoteCallContext<v> {
    public RemoteCallType type;
    public v context;

    public static enum RemoteCallType{ALLOCATE_TASK, MTASK_COMPLETE}

    public RemoteCallContext(RemoteCallType type, v context){
        this.type = type;
        this.context = context;
    }
}
