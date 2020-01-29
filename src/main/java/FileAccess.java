import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class FileAccess
{
    /**
     * Initializes the class, using rootPath as "/" directory
     *
     * @param rootPath - the path to the root of HDFS,
     * for example, hdfs://localhost:32771
     */
    private Configuration configuration;
    private FileSystem hdfs;

    public FileAccess(String rootPath) throws URISyntaxException, IOException {
        configuration = new Configuration();
        configuration.set("dfs.client.use.datanode.hostname", "true");
        System.setProperty("HADOOP_USER_NAME", "root");
        hdfs = FileSystem.get(new URI(rootPath), configuration);

    }

    /**
     * Creates empty file or directory
     *
     * @param path
     */
    public void create(String path) throws IOException {
        hdfs.create(new Path(path));
       /* OutputStream os = hdfs.create(new Path(path));
        BufferedWriter br = new BufferedWriter(
                new OutputStreamWriter(os, "UTF-8")
        );
        br.write("Hello");
        br.flush();
        br.close();*/
    }

    /**
     * Appends content to the file
     *
     * @param path
     * @param content
     */
    public void append(String path, String content) throws IOException {
        Path pathFile = new Path(path);
        InputStream inputStream = hdfs.open(pathFile);
        BufferedReader bfr = new BufferedReader(new InputStreamReader(inputStream));
        String str = null;
        BufferedWriter bwr = new BufferedWriter(new OutputStreamWriter(hdfs.create(pathFile, true)));
        while ((str = bfr.readLine()) != null){
            bwr.write(str);
        }
        bwr.write(content);
        bwr.flush();
        bwr.close();
    }

    /**
     * Returns content of the file
     *
     * @param path
     * @return
     */
    public String read(String path) throws IOException {

        if(!isDirectory(path)){
            InputStream inputStream = hdfs.open(new Path(path));
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            StringBuilder builder = new StringBuilder();
            String line;

            while((line = bufferedReader.readLine()) != null){
                builder.append(line);
            }
            bufferedReader.close();
            return builder.toString();
        }
        return null;
    }

    /**
     * Deletes file or directory
     *
     * @param path
     */
    public void delete(String path) throws IOException {
        Path pathFile = new Path(path);
        if (hdfs.exists(pathFile))
        {
            hdfs.delete(pathFile, true);
        }
    }

    /**
     * Checks, is the "path" is directory or file
     *
     * @param path
     * @return
     */
    public boolean isDirectory(String path) throws IOException
    {


        return hdfs.isDirectory(new Path(path));
    }

    /**
     * Return the list of files and subdirectories on any directory
     *
     * @param path
     * @return
     */
    public List<String> list(String path) throws IOException
    {
        List<String> listFiles = new ArrayList<>();
        RemoteIterator<LocatedFileStatus> files = hdfs.listFiles(new Path(path), true);
        while(files.hasNext())
        {
            listFiles.add(files.next().getPath().toString());
        }
        return listFiles;
    }


    public void close() throws IOException
    {
        hdfs.close();
    }

}
