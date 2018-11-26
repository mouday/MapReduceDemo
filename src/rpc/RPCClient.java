package rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * 客户端
 */
public class RPCClient {
    public static void main(String[] args) throws IOException {
        InetSocketAddress address = new InetSocketAddress("master", 9527);
        Configuration conf = new Configuration();
        Bizable bizable = RPC.getProxy(Bizable.class, 1000, address, conf);

        String result = bizable.sayHello("Tom");

        System.out.println(result);
        RPC.stopProxy(bizable);

    }
}
