import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.framework.api.*;

public class KeyValueHandler implements KeyValueService.Iface, CuratorWatcher {
    private Map<String, String> myMap;
    private Map<String, Integer> versionMap;
    private CuratorFramework curClient;
    private String zkNode;
    private String host;
    private int port;
    private AtomicInteger version;

    // flag to see if primary or backup
    private boolean isPrimary;
    // flag to see if this is there are other nodes in ZK
    private boolean hasBackup;

    void updateFlags() throws Exception {
        while (true) {
            try{
                curClient.sync();
                List<String> children = curClient.getChildren().usingWatcher(this).forPath(zkNode);
                if (children.size() == 0) {
                    throw new Exception("No children present in the ZK directory");
                } else if (children.size() == 1){
                    this.hasBackup = false;
                    this.isPrimary = true;
                } else {
                    InetSocketAddress primaryAddress = this.getPrimary();
                    this.hasBackup = true;
                    this.isPrimary = primaryAddress.getHostName().trim().equals(this.host) && primaryAddress.getPort() == this.port;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    InetSocketAddress getPrimary() throws Exception {
        while (true) {
            curClient.sync();
            List<String> children = curClient.getChildren().usingWatcher(this).forPath(zkNode);
            if (children.size() == 0) {
                throw new Exception("No children present in the ZK directory");
            }
            Collections.sort(children);
            byte[] data = curClient.getData().forPath(zkNode + "/" + children.get(0));
            String strData = new String(data);
            String[] primary = strData.split(":");
            return new InetSocketAddress(primary[0], Integer.parseInt(primary[1]));
        }
    }

    InetSocketAddress getBackup() throws Exception {
        while (true) {
            curClient.sync();
            List<String> children = curClient.getChildren().usingWatcher(this).forPath(zkNode);
            if (children.size() < 2) {
                throw new Exception("Incorrect number of children present in the ZK directory");
            }
            Collections.sort(children);
            byte[] data = curClient.getData().forPath(zkNode + "/" + children.get(1));
            String strData = new String(data);
            String[] primary = strData.split(":");
            return new InetSocketAddress(primary[0], Integer.parseInt(primary[1]));
        }
    }

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
        this.host = host;
        this.port = port;
        this.curClient = curClient;
        this.zkNode = zkNode;
        this.updateFlags();
        this.version = new AtomicInteger(0);
        myMap = new ConcurrentHashMap<String, String>();
        versionMap = new ConcurrentHashMap<String, Integer>();
    }

    public String get(String key) throws org.apache.thrift.TException
    {	
        String ret = myMap.get(key);
        if (ret == null)
            return "";
        else
            return ret;
    }

    public void put(String key, String value) throws org.apache.thrift.TException
    {
        myMap.put(key, value);

        try{
            if (this.isPrimary && this.hasBackup){
                // create a thrift service to propagate kv pair to backup
                TSocket sock = new TSocket(this.host, this.port);
                TTransport transport = new TFramedTransport(sock);
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                KeyValueService.Client client = KeyValueService.Client(protocol);
                client.propagateToBackup(key, value, version.incrementAndGet());
                transport.close();
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public void propagateToBackup(String key, String value, int version){
        // check if version for key is already in backup
        if (versionMap.containsKey(key)){
            // if the version for key is more updated in primary than backup, update
            if (versionMap.get(key) > version){
                myMap.put(key, value);
                versionMap.put(key, version);
            }
        } else{
            myMap.put(key, value);
            versionMap.put(key, version);
        }
    }

    public void dumpToBackup(Map<String, String> myMap, Map<String, Integer> versionMap){
        Iterator itMap = myMap.entrySet().iterator();
        while (itMap.hasNext()){
            Map.Entry pair = (Map.Entry)itMap.next();
            myMap.put((String)pair.getKey(), (String)pair.getValue());
        }

        Iterator itVersion = versionMap.entrySet().iterator();
        while (itVersion.hasNext()){
            Map.Entry pair = (Map.Entry)itVersion.next();
            versionMap.put((String)pair.getKey(), (int)pair.getValue());
        }
    }

    synchronized public void process(WatchedEvent event){
        Watcher.Event.EventType type = event.getType();
        if (type == Watcher.Event.EventType.NodeChildrenChanged) {
            this.updateFlags();
        } else if (type == Watcher.Event.EventType.NodeCreated && this.isPrimary){
            InetSocketAddress backupAddress = this.getBackupAddress();
            try{
                TSocket sock = new TSocket(this.host, this.port);
                TTransport transport = new TFramedTransport(sock);
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                KeyValueService.Client client = KeyValueService.Client(protocol);
                client.dumpToBackup(this.myMap, this.versionMap);
                transport.close();
            } catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}
