import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.net.URI;

public class HDFSFileMetadata {

    public static void main(String[] args) {
        // Replace this with your HDFS file path
        String hdfsUri = "hdfs://localhost:9000";
        String filePath = "/abatra/csvs/small.csv";

        Configuration conf = new Configuration();
        conf.addResource(new Path("/home/abatra/hadoop/hadoop-3.4.0/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/home/abatra/hadoop/hadoop-3.4.0/etc/hadoop/hdfs-site.xml"));
        conf.set("fs.defaultFS", hdfsUri);

        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create(hdfsUri), conf);
            Path path = new Path(filePath);

            if (!fs.exists(path)) {
                System.out.println("File does not exist: " + filePath);
                return;
            }

            FileStatus status = fs.getFileStatus(path);

            System.out.println("Path: " + status.getPath());
            System.out.println("Length: " + status.getLen());
            System.out.println("Owner: " + status.getOwner());
            System.out.println("Group: " + status.getGroup());
            System.out.println("Permission: " + status.getPermission());
            System.out.println("Replication: " + status.getReplication());
            System.out.println("Block Size: " + status.getBlockSize());
            System.out.println("Modification Time: " + status.getModificationTime());
            System.out.println("Access Time: " + status.getAccessTime());

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
