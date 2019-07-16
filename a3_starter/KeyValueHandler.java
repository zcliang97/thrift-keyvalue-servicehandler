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

public class KeyValueHandler implements KeyValueService.Iface {

    public Map<String, MapEntry> myMap;
    public CuratorFramework curClient;
    public String zkNode;
    public String host;
    public int port;

    // flag to see if primary or backup
    public boolean isPrimary;
    // flag to see if this is there are other nodes in ZK
    public boolean hasBackup;

    InetSocketAddress getPrimary() throws Exception {
        while (true) {
            curClient.sync();
            NodeWatcher watcher = new NodeWatcher(this);
            List<String> children = curClient.getChildren().usingWatcher(watcher).forPath(zkNode);
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
            NodeWatcher watcher = new NodeWatcher(this);
            List<String> children = curClient.getChildren().usingWatcher(watcher).forPath(zkNode);
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
        myMap = new ConcurrentHashMap<String, MapEntry>();
    }

    public String get(String key) throws org.apache.thrift.TException
    {	
        // if (!this.isPrimary){
        //     throw new TException("Cannot do get as backup");
        // }
        MapEntry me = myMap.get(key);
        return me != null ? me.getValue() : "";
    }

    public void put(String key, String value) throws org.apache.thrift.TException
    {
        // if (!this.isPrimary){
        //     throw new TException("Cannot do put as backup");
        // }
        MapEntry me = null;
        if (myMap.containsKey(key)){
            me = new MapEntry(value, new AtomicInteger(myMap.get(key).getVersion()).incrementAndGet());
        } else {
            me = new MapEntry(value, 0);
        }
        myMap.put(key, me);
        RPCClient rpc = null;
        try{
            if (this.isPrimary && this.hasBackup){
                // create a thrift service to propagate kv pair to backup
                rpc = RPCCache.getClient();
                rpc.client.propagateToBackup(key, me);
            }
        } catch (org.apache.thrift.TException | InterruptedException e){
            e.printStackTrace();
            rpc = new RPCClient(rpc.host, rpc.port);
        } finally {
            if (rpc != null){
                RPCCache.resetClient(rpc);
            }
        }
    }

    public void propagateToBackup(String key, MapEntry me) throws org.apache.thrift.TException{
        // check if version for key is already in backup
        if (myMap.containsKey(key)){
            // if the version for key is more updated in primary than backup, update
            if (myMap.get(key).getVersion() < me.getVersion()){
                myMap.put(key, me);
            }
        } else {
            myMap.put(key, me);
        }
    }

    public void dumpToBackup(Map<String, MapEntry> myMap) throws org.apache.thrift.TException{
        Iterator itMap = myMap.entrySet().iterator();
        while (itMap.hasNext()){
            Map.Entry pair = (Map.Entry)itMap.next();
            if (!myMap.containsKey(pair.getKey())){
                myMap.put((String)pair.getKey(), (MapEntry)pair.getValue());
            }
        }
    }
}
