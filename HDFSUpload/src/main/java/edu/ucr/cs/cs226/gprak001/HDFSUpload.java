package edu.ucr.cs.cs226.gprak001;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import java.io.IOException;



public class HDFSUpload {

    public  static  void  compareRandomAccess(String path) {
        Configuration conf = new Configuration();
        try {
            Path inputPath = new Path(path);
            FileSystem infs = inputPath.getFileSystem(conf);
            FSDataInputStream inStream = infs.open(inputPath);
            for(int i=0; i<2000; i++) {
                int randomPointer = (int) (Math.random() * ((50) + 1)) + 30;
                inStream.seek(randomPointer);
                for(int j=0; j<1024; j++) {
                    inStream.read();
                }
            }
            inStream.close();

        } catch (Exception e) {
            System.out.println("compare random access failed : " + e);
//            e.printStackTrace();
        }
    }


    public static void readFileAndReturn(String path) throws Exception  {
        Configuration conf = new Configuration();
        byte streamBuffer[] = new byte[256];
        int bytesRead = 0;
        Path inputPath = new Path(path);
        FileSystem infs = inputPath.getFileSystem(conf);
        FSDataInputStream inStream = infs.open(inputPath);
        try{
            while ((bytesRead = inStream.read(streamBuffer)) > 0) {}
        }catch (IOException e) {
            System.out.println("Error while reading the file");
            e.printStackTrace();
        } finally {
            inStream.close();
        }
    }

    public static void compareReadPerformance(String localPath,String HdfsPath) {
        long startTime = System.nanoTime();
        long endTime = 0;
        System.out.println("------------------------------------------------------------");
        try {
            readFileAndReturn(localPath);
            endTime = System.nanoTime();
            System.out.println("Time to read a file in local in nanoseconds " + (endTime - startTime));

            readFileAndReturn(HdfsPath);
            startTime = System.nanoTime();
            endTime = System.nanoTime();
            System.out.println("Time to read a file in HDFS in nanoseconds " + (endTime - startTime));
            System.out.println("------------------------------------------------------------");

        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        Path outputPath = new Path(args[2]);
        Path inputPath = new Path(args[1]);
          FileSystem outfs = outputPath.getFileSystem(conf);
          FileSystem infs = inputPath.getFileSystem(conf);
        if (!infs.exists(inputPath)) {
            System.err.println("input file does not exists");
            return;
        }
        if (outfs.exists(outputPath)) {
            System.err.println("output file already exists");
            return;
        }
        try{
            FSDataInputStream inStream = infs.open(inputPath);
            FSDataOutputStream outStream = outfs.create(outputPath);

            byte streamBuffer[] = new byte[256];
            try {
                int bytesRead = 0;
                long copyTimeStart= System.nanoTime();
                while ((bytesRead = inStream.read(streamBuffer)) > 0) {
                    outStream.write(streamBuffer, 0, bytesRead);
                }
                long copyTimeEnd = System.nanoTime();
                System.out.println("------------------------------------------------------------");
                System.out.println("Time taken to copy from input path to output path in milliseconds: " + ((copyTimeEnd - copyTimeStart)/1000000));

                compareReadPerformance(args[1], args[2]);

                long startTime = System.nanoTime();
                compareRandomAccess(args[1]);
                System.out.println("Time taken to do 2000 random access in local file in milliSeconds : " + (System.nanoTime() - startTime)/1000000);

                startTime = System.nanoTime();
                compareRandomAccess(args[2]);
                System.out.println("Time taken to do 2000 random access in HDFS file in milliSeconds : " + (System.nanoTime() - startTime)/1000000);
                System.out.println("------------------------------------------------------------");


            } catch (IOException e) {
                System.out.println("Error while copying file");
//                e.printStackTrace();
            } finally {
                inStream.close();
                outStream.close();
            }
        }
        catch(IOException e){
            System.out.println("Error while creating input/output stream");
//            e.printStackTrace();
        }
    }
}
