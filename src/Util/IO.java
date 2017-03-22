package Util;

import log.LogSys;
import master.MTask;

import java.io.*;
import java.lang.reflect.Field;
import java.util.ArrayList;

public class IO {
    public static byte[] readBinaryFile(String fileName) throws IOException {

        File f = new File(fileName);
        if (!f.exists()) {
            LogSys.err("File " + fileName + " does not exist!");
            return null;
        }

        byte[] content = new byte[(int) f.length()];
        FileInputStream file = null;

        try {
            file = new FileInputStream(f);

            file.read(content);
        } catch (FileNotFoundException e) {
            LogSys.err("File " + fileName + " does not exist for "+e.toString());
            throw new FileNotFoundException(e.toString());

        } catch (IOException e) {
            LogSys.err("Read file " + fileName + " failed for "+e.toString());
            throw new IOException(e.toString());

        } finally {
            try {
                if(file != null) file.close();
            } catch (IOException e) {
                LogSys.err("Close file " + fileName + " failed for "+e.toString());
                throw new IOException(e.toString());
            }
        }

        return content;
    }



    public static void writeBinaryFile(byte[] content, String fileName) throws IOException {

        int index = fileName.length() - 1;
        while(index >= 0 && fileName.charAt(index) != '/') {
            index--;
        }

        String dir = null;
        if(index > 0){
            dir = fileName.substring(0, index);
            File fileDir = new File(dir);
            if(!fileDir.exists()) {
                fileDir.mkdirs();
            }
        }

        File file = new File(fileName);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                LogSys.err("Create file "+ fileName+"fail");
                throw new IOException(e.toString());
            }
        }

        FileOutputStream fileOutputStream = null;
        try {
            fileOutputStream = new FileOutputStream(file);
            fileOutputStream.write(content);
        } catch (FileNotFoundException e) {
            LogSys.err("File " + fileName + " does not exist for "+e.toString());
            throw new FileNotFoundException(e.toString());
        } catch (IOException e) {
            LogSys.err("Write file " + fileName + " failed for "+e.toString());
            throw new IOException(e.toString());
        } finally {
            try {
                if(fileOutputStream == null) fileOutputStream.close();
            } catch (IOException e) {
                LogSys.err("Close file " + fileName + " failed for "+e.toString());
                throw new IOException(e.toString());
            }
        }
    }

    public static ArrayList<String> readTextFileByLine(String fileName) throws IOException {

        ArrayList<String> lines = new ArrayList<>();
        if (fileName == null) {
            LogSys.debug("fileName is null");
        }

        BufferedReader reader = null;
        int index = 0;
        try {
            reader = new BufferedReader(new FileReader(fileName));
            String line = reader.readLine();
            while (line != null) {
                lines.add(line);
                line = reader.readLine();
            }

        } catch (FileNotFoundException e) {
            LogSys.err("File " + fileName + " does not exist for " + e.toString());
            throw new FileNotFoundException(e.toString());
        } catch (IOException e) {
            LogSys.err("Read file " + fileName + " failed for " + e.toString());
            throw new IOException(e.toString());
        } finally {
            try {
                if (reader != null) reader.close();
            } catch (IOException e) {
            }
            return lines;
        }
    }
}
