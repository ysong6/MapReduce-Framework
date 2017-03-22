package Util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.SortedMap;
import java.util.TreeMap;

public class UserMethod implements Serializable {
    private static final long serialVersionUID =1;
    public Class<?> userClass;
    public String classFilePath;

    public UserMethod(Class<?> c, String path){
        userClass = c;
        classFilePath = path;
    }

    public Class<?> getUserClass() {
        return userClass;
    }

    public String getClassFilePath() {
        return classFilePath;
    }

}
