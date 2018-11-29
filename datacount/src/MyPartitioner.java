import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

public class MyPartitioner extends Partitioner<Text, DataBean> {

    private static Map<String, Integer> providerMap = new HashMap<String, Integer>();

    static {
        providerMap.put("134", 1);
        providerMap.put("135", 1);
        providerMap.put("136", 1);
        providerMap.put("137", 1);
        providerMap.put("138", 1);
        providerMap.put("139", 1);

        providerMap.put("150", 2);
        providerMap.put("159", 2);

        providerMap.put("182", 3);
        providerMap.put("183", 3);
    }

    @Override
    public int getPartition(Text text, DataBean dataBean, int i) {
        String account = text.toString();
        String sub_acc = account.substring(0, 3);
        Integer code = providerMap.get(sub_acc);
        if (code == null) {
            code = 0;
        }
        return code;
    }
}
