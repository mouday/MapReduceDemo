import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

public class HBaseDemo {
    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "116.196.95.96");

        HBaseAdmin admin = new HBaseAdmin(config);

        HTableDescriptor htd = new HTableDescriptor("user");
        HColumnDescriptor hcd_info = new HColumnDescriptor("info");
        hcd_info.setMaxVersions(3);
        HColumnDescriptor hcd_data = new HColumnDescriptor("data");
        htd.addFamily(hcd_info);
        htd.addFamily(hcd_data);
        admin.createTable(htd);
        admin.close();
    }
}
