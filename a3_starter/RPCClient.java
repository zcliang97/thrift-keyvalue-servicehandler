import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.framework.api.*;

public class RPCClient {
    public KeyValueService.Client client;
    public TTransport transport;
    public String host;
    public int port;
    public RPCClient(String host, int port){
        this.client = null;
        this.host = host;
        this.port = port;
        try{
            TSocket sock = new TSocket(host, port);
            transport = new TFramedTransport(sock);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            this.client = new KeyValueService.Client(protocol);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public void close(){
        try{
            if (transport != null && transport.isOpen()){
                transport.close();
            }
        } catch (Exception e){
            // e.printStackTrace();
            StorageNode.log.info("Transport Exception");
        }
    }
}