package worker;

import master.MTask;

/**
 * This is the interface for mapperMethod code.
 */

public interface Mapper{
    void  map(int key, String value, MTask context);
}
