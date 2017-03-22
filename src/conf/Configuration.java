package conf;

import Util.IO;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;

public class Configuration {

    //parse configuration properties for objects
    public static void configParser(String filename, Object obj) throws IOException {

        ArrayList<String> conf = null;
        try {
            conf = IO.readTextFileByLine(filename);
        } catch (FileNotFoundException e) {
            throw new IOException(e.toString());
        } catch (IOException e) {
            throw new IOException(e.toString());
        }

        for (String line : conf) {
            int index = line.length()-1;
            while(index > 0 && line.charAt(index) != '=') index--;
            if (index <= 0) {
                continue;
            }
            String properties = line.substring(0, index).trim();
            String value = line.substring(index+1, line.length()).trim();

            try {
                Field field = obj.getClass().getDeclaredField(properties);
                field.setAccessible(true);
                if (field.getType().isPrimitive()) {
                    field.setInt(obj, Integer.parseInt(value));
                } else if (field.getType().equals(String.class)) {
                    field.set(obj, value);
                } else if (field.getType().equals(Integer.class)) {
                    field.set(obj, Integer.parseInt(value));
                } else if (field.getType().equals(Double.class)) {
                    field.set(obj, Double.parseDouble(value));
                }
            } catch (NoSuchFieldException e) {
                continue;
            } catch (SecurityException e) {
                throw new IOException(e.toString());
            } catch (NumberFormatException e) {
                continue;
            } catch (IllegalArgumentException e) {
                throw new IOException(e.toString());
            } catch (IllegalAccessException e) {
                throw new IOException(e.toString());
            }
        }
    }

}
