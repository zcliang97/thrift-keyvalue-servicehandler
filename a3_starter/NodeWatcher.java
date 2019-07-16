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

public class NodeWatcher implements CuratorWatcher{
    public KeyValueHandler handler;

    public NodeWatcher(KeyValueHandler handler){
        this.handler = handler;
    }

    public void updateFlags() throws Exception {
        try{
            handler.curClient.sync();
            List<String> children = handler.curClient.getChildren().usingWatcher(this).forPath(handler.zkNode);
            if (children.size() == 0) {
                throw new Exception("No children present in the ZK directory");
            } else if (children.size() == 1){
                handler.hasBackup = false;
                handler.isPrimary = true;
            } else if (children.size() == 2) {
                InetSocketAddress primaryAddress = handler.getPrimary();
                handler.hasBackup = true;
                handler.isPrimary = primaryAddress.getHostName().trim().equals(handler.host) && primaryAddress.getPort() == handler.port;
            } else {
                StorageNode.log.info(String.format("ERROR: There are %d children.", children.size()));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    synchronized public void process(WatchedEvent event) throws Exception {
        try{
            this.updateFlags();
            StorageNode.log.info(handler.isPrimary ? "Role is: Primary" : "Role is: Backup");
            if (handler.isPrimary && handler.hasBackup){
                InetSocketAddress address = handler.getBackup();
                StorageNode.log.info("created clients on: " + address.getHostName() + ", " + String.valueOf(address.getPort()));
                RPCCache.createClients(address.getHostName(), Integer.valueOf(address.getPort()));

                handler.dumpToBackup(handler.myMap);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}