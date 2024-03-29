import java.net.InetSocketAddress;
import java.io.*;
import java.util.*;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.utils.*;

import org.apache.log4j.*;

public class StorageNode {
	static Logger log;
	static boolean isPrimary;
	
    public static void main(String [] args) throws Exception {
		BasicConfigurator.configure();
		log = Logger.getLogger(StorageNode.class.getName());

		if (args.length != 4) {
			System.err.println("Usage: java StorageNode host port zkconnectstring zknode");
			System.exit(-1);
		}

		CuratorFramework curClient =
			CuratorFrameworkFactory.builder()
			.connectString(args[2])
			.retryPolicy(new RetryNTimes(10, 1000))
			.connectionTimeoutMs(1000)
			.sessionTimeoutMs(10000)
			.build();

		curClient.start();
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				curClient.close();
			}
			});

		KeyValueHandler handler = new KeyValueHandler(args[0], Integer.parseInt(args[1]), curClient, args[3]);
		KeyValueService.Processor<KeyValueService.Iface> processor = new KeyValueService.Processor<>(handler);
		TNonblockingServerTransport socket = new TNonblockingServerSocket(Integer.parseInt(args[1]));
		THsHaServer.Args sargs = new THsHaServer.Args(socket);
		sargs.protocolFactory(new TBinaryProtocol.Factory());
		sargs.transportFactory(new TFramedTransport.Factory());
		// sargs.inputTransportFactory(new TFramedTransport.Factory());
		// sargs.outputTransportFactory(new TFramedTransport.Factory());
		sargs.processorFactory(new TProcessorFactory(processor));
		sargs.maxWorkerThreads(64);
		TServer server = new THsHaServer(sargs);
		log.info("Launching server");

		new Thread(new Runnable() {
			public void run() {
				server.serve();
			}
			}).start();

		// create an ephemeral node in ZooKeeper
		String host = args[0];
		String port = args[1];
		String zkNode = args[3];
		String data = host + ":" + port;
		curClient.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(zkNode + "/", data.getBytes());

		NodeWatcher watcher = new NodeWatcher(handler);
		List<String> children = curClient.getChildren().usingWatcher(watcher).forPath(args[3]);

		watcher.updateFlags();
		log.info(watcher.handler.isPrimary ? "Role is: Primary" : "Role is: Backup");
		if (watcher.handler.isPrimary && watcher.handler.hasBackup){
			InetSocketAddress address = watcher.handler.getBackup();
			log.info("created clients on: " + address.getHostName() + ", " + String.valueOf(address.getPort()));
			RPCCache.createClients(address.getHostName(), Integer.valueOf(address.getPort()));
		}
	}
}
