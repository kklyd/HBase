package com.ruch.cloud.project;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
//hello
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.generated.master.table_jsp;
import org.apache.hadoop.hbase.util.Bytes;

public class HBase {
    private final static byte[] cellData = Bytes.toBytes("cell_data");

    /** Drop tables if this value is set true. */
    static boolean INITIALIZE_AT_FIRST = false;

    private static void p(String msg) {
        System.out.println(msg);
    }

    /**
     * <table> timeTable
     * <tr>
     * <tb>bucket|createTime|ID(00|20131111000000|XXXXXXX)</tb> <tb>tweet:text</tb>
     * </tr>
     * </table>
     */
    /**
     * <table> userTable
     * <tr>
     * <tb>bucket|createTime|ID(00|XXXX|XXXXXXX)</tb> <tb>tweet:re_id</tb> <tb>tweet:re_userid</tb>
     * </tr>
     * </table>
     */
    
    
    private byte[] table, row, family, qualifier1, qualifier2; //
    
    public static final String MASTER_IP = "localhost";
    //public static final String ZOOKEEPER_PORT = "2181";
    public static final String ZOOKEEPER_PORT = "60000";
    
    private String tableName;
    
    public HBase(String TableName){
    	tableName = TableName;
    	if (TableName.equals("timeTable")) {
    		table = Bytes.toBytes(TableName);
    		qualifier1 = Bytes.toBytes("text");
    		family = Bytes.toBytes("tweet");
		}else if(TableName.equals("userTable")) {
    		table = Bytes.toBytes(TableName);
    		qualifier1 = Bytes.toBytes("re_id");
    		qualifier2 = Bytes.toBytes("re_userid");
    		family = Bytes.toBytes("tweet");
		}
    }

    
    private HBaseAdmin connectionHBase() throws IOException {
    	
    	Configuration config = HBaseConfiguration.create();
    	config.set("hbase.zookeeper.quorum", MASTER_IP);
    	config.set("hbase.zookeeper.property.clientPort", ZOOKEEPER_PORT);
    	p("Running connecting test...");
    	try {
    	    HBaseAdmin admin = new HBaseAdmin(config);
    	    p("HBase found!");
    	    
    	    return admin;
    	} catch (MasterNotRunningException e) {
    	    p("HBase connection failed!");
    	    e.printStackTrace();
    	} catch (ZooKeeperConnectionException e) {
    	    p("Zookeeper connection failed!");
    	    e.printStackTrace();
    	} catch (Exception e) { 
    		e.printStackTrace(); 
    	}
    	
    	return null;
    } 
   
    
    private void createTable(HBaseAdmin admin) throws IOException {
        HTableDescriptor desc = new HTableDescriptor(table);
        desc.addFamily(new HColumnDescriptor(family));
        admin.createTable(desc);
    }

    private void deleteTable(HBaseAdmin admin) throws IOException {
        if (admin.tableExists(table)) {
            admin.disableTable(table);
            try {
                admin.deleteTable(table);
            } finally {
            }
        }
    }

    private void filters(HBaseAdmin admin, HTableInterface table) throws IOException {
        p("\n*** FILTERS ~ scanning with filters to fetch a row of which key is larget than \"Row1\"~ ***");
        Filter filter1 = new PrefixFilter(row);
        Filter filter2 = new QualifierFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator(
                qualifier1));

        List<Filter> filters = Arrays.asList(filter1, filter2);
        Filter filter3 = new FilterList(Operator.MUST_PASS_ALL, filters);

        Scan scan = new Scan();
        scan.setFilter(filter3);

        ResultScanner scanner = table.getScanner(scan);
        try {
            int i = 0;
            for (Result result : scanner) {
                p("Filter " + scan.getFilter() + " matched row: " + result);
                i++;
            }
            assert i == 1 : "This filtering sample should return 1 row but was " + i + ".";
        } finally {
            scanner.close();
        }
        p("Done. ");
    }

    private void get(HBaseAdmin admin, HTableInterface table) throws IOException {
        p("\n*** GET example ~fetching the data in Family1:Qualifier1~ ***");

        Get g = new Get(row);
        Result r = table.get(g);
        byte[] value = r.getValue(family, qualifier1);

        p("Fetched value: " + Bytes.toString(value));
        assert Arrays.equals(cellData, value);
        p("Done. ");
    }

    private void put(HBaseAdmin admin, HTableInterface table) throws IOException {
        p("\n*** PUT example ~inserting \"cell-data\" into Family1:Qualifier1 of Table1 ~ ***");

        // Row1 => Family1:Qualifier1, Family1:Qualifier2
        Put p = new Put(row);
        p.add(family, qualifier1, cellData);
        p.add(family, qualifier2, cellData);
        table.put(p);
        
       admin.disableTables(tableName);
        
        try {
            HColumnDescriptor desc = new HColumnDescriptor(row);
            admin.addColumn(tableName, desc);
            p("Success.");
        } catch (Exception e) {
            p("Failed.");
        } finally {
            admin.enableTable(tableName);
        }
        p("Done. ");
    }

    public void run(Configuration config) throws IOException {
        HBaseAdmin admin = connectionHBase();
        if (admin == null){
        	p("Connection fail...");
        	return;
        }
        HTableFactory factory = new HTableFactory();
        if (INITIALIZE_AT_FIRST) {
            deleteTable(admin);
        }

        if (!admin.tableExists(table)) {
            createTable(admin);
        }

        HTableInterface Htable = factory.createHTableInterface(config, table);
        put(admin, Htable);
        get(admin, Htable);
        scan(admin, Htable);
        filters(admin, Htable);
        //delete(admin, table);
        factory.releaseHTableInterface(Htable); // Disconnect
    }

    private void scan(HBaseAdmin admin, HTableInterface table) throws IOException {
        p("\n*** SCAN example ~fetching data in Family1:Qualifier1 ~ ***");

        Scan scan = new Scan();
        scan.addColumn(family, qualifier1);

        ResultScanner scanner = table.getScanner(scan);
        try {
            for (Result result : scanner)
                p("Found row: " + result);
        } finally {
            scanner.close();
        }
        p("Done.");
    }
}
