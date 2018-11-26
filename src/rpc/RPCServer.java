package rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * 服务端
 * 打包为可执行jar
 * 运行：java -jar hdfsdemo.jar
 */
public class RPCServer implements Bizable {

    public String sayHello(String name) {
        return "Hello " + name;
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();

        RPC.Builder builder = new RPC.Builder(conf);
        builder.setProtocol(Bizable.class);
        builder.setInstance(new RPCServer());
        builder.setBindAddress("master");
        builder.setPort(9527);

        RPC.Server server = builder.build();
        server.start();
    }
}
