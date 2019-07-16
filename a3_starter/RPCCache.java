import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class RPCCache {
    public static final int MAX_RPC_NUM = 16;
    public static BlockingQueue<RPCClient> clients;

    public static void createClients(String host, int port){
        if (clients != null){
            for (RPCClient c : clients){
                c.close();
            }
        }
        clients = new LinkedBlockingQueue<RPCClient>(MAX_RPC_NUM);
        try {
            for (int i = 0; i < MAX_RPC_NUM; i++){
                clients.put(new RPCClient(host, port));
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public static RPCClient getClient() throws InterruptedException {
        try{
            return clients.poll(10L, TimeUnit.SECONDS);
        } catch (NullPointerException e){
            throw e;
        }
        
    }

    public static void resetClient(RPCClient client) {
        try{
            clients.put(client);
        } catch (Exception e){
            e.printStackTrace();
        }
        
    }
}