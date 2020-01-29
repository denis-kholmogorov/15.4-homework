import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.List;

public class Main
{
    private static String symbols = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    public static void main(String[] args) throws Exception
    {
        String root = "hdfs://2e3215d83093:8020";
        FileAccess fileAccess = new FileAccess(root);
        String path1 = root + "/test/file1.txt";
        String path2 = root + "/test/file2.txt";
        String path3 = root + "/test/files/file/";

        fileAccess.create(path1);
        fileAccess.create(path2);
        fileAccess.create(path3);

        System.out.println("- isDirectory - " + fileAccess.isDirectory(path1));
        System.out.println("- isDirectory - " + fileAccess.isDirectory(path2));
        System.out.println("- isDirectory - " + fileAccess.isDirectory(root+"/test/files/"));

        fileAccess.append(path1, "Hadoob");
        System.out.println("- ADD in first file - Hadoob");
        fileAccess.append(path2, "Hello");
        System.out.println("- ADD in second file - Hello");

        System.out.println("- READ first file - " + fileAccess.read(path1));
        System.out.println("- READ second file - " + fileAccess.read(path2));

        fileAccess.append(path2, " World");
        System.out.println("- ADD in second file - World");

        System.out.println("- READ second file - " + fileAccess.read(path2));

        fileAccess.delete(path1);
        System.out.println("- DELETE - first file");

        List<String> listFiles = fileAccess.list(root + "/");
        for (String file: listFiles){
            System.out.println(file);
        }
        fileAccess.close();
    }
        /*Configuration configuration = new Configuration();
        configuration.set("dfs.client.use.datanode.hostname", "true");
        System.setProperty("HADOOP_USER_NAME", "root");

        FileSystem hdfs = FileSystem.get(
            new URI("hdfs://2e3215d83093:8020"), configuration
        );
        Path file = new Path("hdfs://2e3215d83093:8020/test/file.txt");

        if (hdfs.exists(file)) {
            hdfs.delete(file, true);
        }

        OutputStream os = hdfs.create(file);
        BufferedWriter br = new BufferedWriter(
            new OutputStreamWriter(os, "UTF-8")
        );

        for(int i = 0; i < 10_000_000; i++) {
            br.write(getRandomWord() + " ");
        }

        br.flush();
        br.close();
        hdfs.close();
    }

    private static String getRandomWord()
    {
        StringBuilder builder = new StringBuilder();
        int length = 2 + (int) Math.round(10 * Math.random());
        int symbolsCount = symbols.length();
        for(int i = 0; i < length; i++) {
            builder.append(symbols.charAt((int) (symbolsCount * Math.random())));
        }
        return builder.toString();
    }*/
}
