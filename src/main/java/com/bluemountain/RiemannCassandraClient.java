package com.bluemountain;

import java.io.PrintWriter;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutorMBean;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.aphyr.riemann.Proto.Event;
import com.aphyr.riemann.client.RiemannClient;

/**
 * Monitoring client that polls Cassandra MBeans for metrics and streams them to
 * riemann as events
 * 
 * @author jluciani
 * 
 */
public class RiemannCassandraClient {

    private static Options options = null;

    static {
	options = new Options();

	options.addOption("riemann_host", true, "hostname for riemann server");
	options.addOption("riemann_port", true, "port number for riemann server");
	options.addOption("cassandra_host", true, "hostname for cassandra node");
	options.addOption("jmx_port", true, "port number for jmx on cassandra node");
	options.addOption("jmx_username", true, "username for cassandra jmx agent");
	options.addOption("jmx_password", true, "password cassandra jmx agent");
	options.addOption("interval_seconds", true, "number of seconds between updates");
	options.addOption("tp", true, "emit ThreadPoolMetrics");
	options.addOption("cf", true, "emit ColumnFamilyMetrics");
	options.addOption("bs", true, "emit BasicMetrics");
	options.addOption("column_family1", true, "1st column family to monitor");
	options.addOption("column_family2", true, "2nd column family to monitor");
	options.addOption("column_family3", true, "3rd column family to monitor");
    }

    final RiemannClient riemannClient;
    volatile NodeProbe jmxClient = null;

    final String cassandraHost;
    final Integer cassandraJmxPort;
    final String jmxUsername;
    final String jmxPassword;
    final Event protoEvent;

    public RiemannCassandraClient(String riemannHost, Integer riemannPort, String cassandraHost, Integer cassandraJmxPort, String jmxUsername,
	    String jmxPassword) {

	this.cassandraHost = cassandraHost;
	this.cassandraJmxPort = cassandraJmxPort;
	this.jmxUsername = jmxUsername;
	this.jmxPassword = jmxPassword;

	protoEvent = Event.newBuilder().setHost(pickBestHostname(cassandraHost)).addTags("cassandra").setState("ok").setTtl(5).build();

	riemannClient = new RiemannClient(new InetSocketAddress(riemannHost, riemannPort));

	if (!reconnectJMX())
	    System.err.println(String.format("Unable to connect to Cassandra JMX (%s:%d) will continue to try silently....", cassandraHost, cassandraJmxPort));
    }

    private String pickBestHostname(String cassandraHost)
    {
	try {	    
	    InetAddress cassandraAddr = InetAddress.getByName(cassandraHost);

	    if (!cassandraAddr.isLoopbackAddress()) 
		return cassandraAddr.getCanonicalHostName();
	    
	    //Pick first non local ip with a hostname
	    Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
	    for (NetworkInterface netint : Collections.list(nets))
	    {
		Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
	        for (InetAddress inetAddress : Collections.list(inetAddresses)) {
	            if (!inetAddress.isLoopbackAddress() && inetAddress instanceof Inet4Address)
	        	return inetAddress.getCanonicalHostName();
	        }
	    }
	    
	} catch (UnknownHostException e) {
	    throw new RuntimeException("Unknown host", e);
	} catch (SocketException e) {
	    throw new RuntimeException("Error getting network info", e);
	}
	
	return cassandraHost;
    }
    
    private synchronized boolean reconnectJMX() {

	try {
	    if (jmxUsername == null)
		jmxClient = new NodeProbe(cassandraHost, cassandraJmxPort);
	    else
		jmxClient = new NodeProbe(cassandraHost, cassandraJmxPort, jmxUsername, jmxPassword);

	    return true;
	} catch (Exception e) {
	    // We silently continue
	    //e.printStackTrace();
	}

	jmxClient = null;
	return false;
    }

    private void add(List<Event> events, String name, float val) {
	events.add(Event.newBuilder(protoEvent).setService(name).setMetricF(val).build());
    }

    private void add(List<Event> events, String name, float val, String desc) {
	events.add(Event.newBuilder(protoEvent).setService(name).setMetricF(val).setDescription(desc).build());
    }

    private void add(List<Event> events, String name, float val, String desc, String tag) {
    	events.add(Event.newBuilder(protoEvent).setService(name).setMetricF(val).setDescription(desc).addTags(tag).build());
    }

    private long emitColumnFamilyMetrics(String columnFamily1, String columnFamily2, String columnFamily3) {
	List<Event> events = new ArrayList<Event>();

	Iterator<Entry<String, ColumnFamilyStoreMBean>> it = jmxClient.getColumnFamilyStoreMBeanProxies();

	// CF metrics
	long totalBytes = 0;
	while (it.hasNext()) {
	    Entry<String, ColumnFamilyStoreMBean> e = it.next();

	    String name = "c.db." + e.getKey() + "." + e.getValue().getColumnFamilyName();
	    ColumnFamilyStoreMBean v = e.getValue();

	    if(columnFamily1.equals("all") || 
	    	columnFamily1.equals(v.getColumnFamilyName()) || 
	    	columnFamily2.equals(v.getColumnFamilyName()) ||
	    	columnFamily3.equals(v.getColumnFamilyName())) {

		    add(events, name + ".keys", v.estimateKeys() / 1000, "keys", v.getColumnFamilyName());
		    add(events, name + ".total_sstable_mb", v.getLiveDiskSpaceUsed() / (1024 * 1024),"total_sstable_mb",v.getColumnFamilyName());
		    add(events, name + ".total_bloom_mb", v.getBloomFilterDiskSpaceUsed() / (1024 * 1024),"total_bloom_mb",v.getColumnFamilyName());
		    add(events, name + ".bloom_fp_rate", (float) v.getRecentBloomFilterFalseRatio(),"bloom_fp_rate",v.getColumnFamilyName());
		    add(events, name + ".max_row_size_kb", v.getMaxRowSize() / 1024,"max_row_size_kb",v.getColumnFamilyName());
		    add(events, name + ".min_row_size_kb", v.getMinRowSize() / 1024,"min_row_size_kb",v.getColumnFamilyName());
		    add(events, name + ".mean_row_size_kb", v.getMeanRowSize() / 1024,"mean_row_size_kb",v.getColumnFamilyName());
		    add(events, name + ".sstable_count", v.getLiveSSTableCount(),"sstable_count",v.getColumnFamilyName());
		    add(events, name + ".memtable_size_mb", (float) v.getMemtableDataSize() / (1024 * 1024),"memtable_size_mb",v.getColumnFamilyName());
		    add(events, name + ".memtable_count", (float) v.getMemtableColumnsCount(),"memtable_count",v.getColumnFamilyName());
		    add(events, name + ".memtable_switch_count", (float) v.getMemtableSwitchCount(),"memtable_switch_count",v.getColumnFamilyName());
		    

		    // latencies can return NaN
		    Float f = (float) v.getRecentReadLatencyMicros();
		    add(events, name + ".read_latency", f.equals(Float.NaN) ? 0.0f : f,"read_latency",v.getColumnFamilyName());

		    f = (float) e.getValue().getRecentWriteLatencyMicros();
		    add(events, name + ".write_latency", f.equals(Float.NaN) ? 0.0f : f,"write_latency",v.getColumnFamilyName());

		    totalBytes += e.getValue().getLiveDiskSpaceUsed();

		    riemannClient.sendEvents(events.toArray(new Event[] {}));
		    events.clear();
		}
	}

	return totalBytes;
    }

    private void emitThreadPoolMetrics() {

	List<Event> events = new ArrayList<Event>();

	Iterator<Entry<String, JMXEnabledThreadPoolExecutorMBean>> it = jmxClient.getThreadPoolMBeanProxies();
	while (it.hasNext()) {
	    Entry<String, JMXEnabledThreadPoolExecutorMBean> p = it.next();

	    String name = "c.tp." + p.getKey();
	    JMXEnabledThreadPoolExecutorMBean v = p.getValue();

	    add(events, name + ".completed", v.getCompletedTasks(), "completed", p.getKey());
	    add(events, name + ".active", v.getActiveCount(),"active",p.getKey());
	    add(events, name + ".pending", v.getPendingTasks(),"pending",p.getKey());
	    add(events, name + ".blocked", v.getCurrentlyBlockedTasks(),"blocked",p.getKey());

	    riemannClient.sendEvents(events.toArray(new Event[] {}));
	    events.clear();
	}
    }

    private void emitMetrics(boolean tp, boolean cf, boolean bs, String columnFamily1, String columnFamily2, String columnFamily3) {

	if (jmxClient == null && !reconnectJMX())
	    return;

	try {

		List<Event> events = new ArrayList<Event>();

	    if (tp)// TP Metrics
	    	emitThreadPoolMetrics();


	    // CF Metrics
	    long totalSSTableBytes; 

	    if (cf)
	    	totalSSTableBytes = emitColumnFamilyMetrics(columnFamily1, columnFamily2, columnFamily3);
	    else
	    	totalSSTableBytes = 0;

	    // Basic metrics
	    
	    if (bs) {
		    add(events, "c.exception_count", jmxClient.getExceptionCount());
		    add(events, "c.heap_used_mb", jmxClient.getHeapMemoryUsage().getUsed() / (1024 * 1024));
		    add(events, "c.heap_max_mb", jmxClient.getHeapMemoryUsage().getMax() / (1024 * 1024));
		    add(events, "c.heap_committed_mb", jmxClient.getHeapMemoryUsage().getCommitted() / (1024 * 1024));
		    add(events, "c.recent_timeouts", jmxClient.msProxy.getRecentTotalTimouts(), FBUtilities.json(jmxClient.msProxy.getRecentTimeoutsPerHost()));
		    add(events, "c.pending_compactions", jmxClient.getCompactionManagerProxy().getPendingTasks());
		    add(events, "c.total_sstable_mb", totalSSTableBytes / (1024 * 1024));
		}

	    riemannClient.sendEvents(events.toArray(new Event[] {}));
	} catch (Throwable t) {
	    // Try again later
            jmxClient = null;
	    t.printStackTrace();
	}
    }

    public static void printUsage() {
	final PrintWriter writer = new PrintWriter(System.out);
	final HelpFormatter usageFormatter = new HelpFormatter();
	usageFormatter.printUsage(writer, 80, "riemann-cassandra", options);
	writer.close();
    }

    public static void main(String[] args) {

	BasicParser parser = new BasicParser();
	CommandLine cl = null;

	try {
	    cl = parser.parse(options, args);
	} catch (ParseException e) {
	    printUsage();
	    System.exit(1);
	}

	// Extracted options
	String cassandraHost = cl.getOptionValue("cassandra_host", "localhost");
	String riemannHost = cl.getOptionValue("riemann_host", "localhost");
	String jmxUsername = cl.getOptionValue("jmx_username");
	String jmxPassword = cl.getOptionValue("jmx_password");
	Integer jmxPort = Integer.valueOf(cl.getOptionValue("jmx_port", "7199"));
	Integer riemannPort = Integer.valueOf(cl.getOptionValue("riemann_port", "5555"));
	Integer intervalSeconds = Integer.valueOf(cl.getOptionValue("interval_seconds", "5"));
	boolean doTPMetrics = Boolean.parseBoolean(cl.getOptionValue("tp","false"));
	boolean doCFMetrics = Boolean.parseBoolean(cl.getOptionValue("cf","false"));
	boolean doBSMetrics = Boolean.parseBoolean(cl.getOptionValue("bs","true"));
	String columnFamily1 = cl.getOptionValue("column_family1", "all");
	String columnFamily2 = cl.getOptionValue("column_family2", "none");
	String columnFamily3 = cl.getOptionValue("column_family3", "none");

	RiemannCassandraClient cli = new RiemannCassandraClient(riemannHost, riemannPort, cassandraHost, jmxPort, jmxUsername, jmxPassword);

	while (true) {
	    cli.emitMetrics(doTPMetrics, doCFMetrics, doBSMetrics, columnFamily1, columnFamily2, columnFamily3);
	    try {
		Thread.sleep(intervalSeconds * 1000);
	    } catch (InterruptedException e) {
	    	e.printStackTrace();
	    	break;
	    }
	}
    }
}
