import java.util.List;

public class Main
{
    private static String symbols = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    public static void main(String[] args) throws Exception
    {
        String root = "hdfs://47dc74575f36:8020";
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
}
