package org.apache.zookeeper.server;

import java.nio.charset.StandardCharsets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.Assert;

@RunWith(Enclosed.class)
public class DataTreeTest {
	
	@RunWith(Parameterized.class)
	public static class CreateNodeTest {
		private String path;
		private byte[] data;
		private long ephemeralOwner;
		private int parentCVersion;
		private long zxid;
		private long time;
		private List<ACL> acl;
		private Stat outputStat;
		
		private String expectedResult;
		
		private DataTree dataTree;
		
		@Parameters
		public static Collection<Object[]> data() {
			byte[] emptyData = { };
			List<ACL> aclList = new ArrayList<ACL>();
			List<ACL> emptyList = new ArrayList<ACL>();
			ACL acl = new ACL();
			acl.setPerms(0);
			aclList.add(acl);
						
	        return Arrays.asList(new Object[][] {
	        	{ null, null, null, 0, 0, 0, 0, null, null },
	        	{ "nodo", null, null, 0, 0, 0, 0, null, "begin 0, end -1, length 4" },
	        	{ "/nodo", emptyData, null, 1, 0, 0, 0, null, "1" },
	        	{ "/nodo", null, null, 0, 0, 0, 0, null, "1" },
	        	{ "/nodo", "ciao".getBytes(), null, -1, 0, 0, 0, null, "1"},
	        	{ "/nodo", "ciao".getBytes(), null, 0, 0, 0, 0, null, "1" },
	        	{ "/nodo", "ciao".getBytes(), null, 0, -1, 0, 0, null, "1" },
	        	{ "/nodo", "ciao".getBytes(), null, 0, 0, -1, 0, null, "1" },
	        	{ "/nodo", "ciao".getBytes(), null, 17, 0, 0, 0, null, "1" },
	        	{ "/nodo", "ciao".getBytes(), null, -1, 1, 0, 0, null, "1" },
	        	{ "/nodo", "ciao".getBytes(), null, -1, -1, 0, 0, null, "1" },
	        	{ "/nodo", "ciao".getBytes(), null, 0, -1, 0, 0, null, "1" },
	        	{ "/nodo", "ciao".getBytes(), null, -1, 1, 0, 1, null, "1" },
	        	{ "/nodi/nodo", "ciao".getBytes(), null, -1, 1, 0, 1, null, "KeeperErrorCode = NoNode" },
	        	{ "/exist", "ciao".getBytes(), null, -1, 1, 0, 1, null, "KeeperErrorCode = NodeExists" },
	        	{ "/nodo", "ciao".getBytes(), emptyList, Long.MIN_VALUE, 1, 0, 1, null, "1" },	        	
	        	{ "/prop", "ciao".getBytes(), emptyList, -1, 1, 0, 1, null, "1" },
	        	{ "/nodo", "ciao".getBytes(), emptyList, -1, 1, 0, 1, null, "1" },
	        	{ "/nodo", "ciao".getBytes(), aclList, -1, 1, 0, 1, null, null },
	        	{ "/nodo", "ciao".getBytes(), null, 1, 1, 0, -1, new Stat(), "1" },
	        	{ "/nodo", "ciao".getBytes(), null, -1, 1, 0, 0, new Stat(), "1" },
	        	{ "/zookeeper/quota/nodo/zookeeper_limits", "ciao".getBytes(), emptyList, -1, 1, 0, 1, null, "1" },
	        	{ "/zookeeper/quota/zookeeper_stats", "ciao".getBytes(), emptyList, -1, 1, 0, 1, null, "1" }
	        });
	    }

		public CreateNodeTest(String path, byte[] data, List<ACL> acl, long ephemeralOwner, int parentCVersion, long zxid, long time, Stat outputStat, String expectedResult) {
			configure(path, data, acl, ephemeralOwner, parentCVersion, zxid, time, outputStat, expectedResult);
		}
		
		public void configure(final String path, byte[] data, List<ACL> acl, long ephemeralOwner, int parentCVersion, long zxid, long time, Stat outputStat, String expectedResult) {
			this.path = path;
			this.data = data; 
			this.acl = acl;
			this.ephemeralOwner = ephemeralOwner;
			this.parentCVersion = parentCVersion;
			this.zxid = zxid;
			this.time = time;
			this.outputStat = outputStat;
			this.expectedResult = expectedResult;
			
			dataTree = new DataTree();
			
			if (path != null && path.equals("/exist")) {
				try {
					dataTree.createNode("/exist", data, acl, ephemeralOwner, parentCVersion, zxid, time);
				} catch (NoNodeException | NodeExistsException e) {
					e.printStackTrace();
				}
			} else if (path != null && path.equals("/prop")) {
				System.setProperty("zookeeper.extendedTypesEnabled", "true");
				System.setProperty("zookeeper.emulate353TTLNodes", "true");
			} else if (ephemeralOwner == 17) {
				try {
					dataTree.createNode("/parent", data, acl, ephemeralOwner, parentCVersion, zxid, time);
				} catch (NoNodeException | NodeExistsException e) {
					e.printStackTrace();
				}
			} else if (path != null && path.equals("/zookeeper/quota/nodo/zookeeper_limits")) {
				try {
					dataTree.createNode("/zookeeper/quota/nodo", null, acl, ephemeralOwner, parentCVersion, zxid, time);
					dataTree.createNode("/zookeeper/quota/nodo/file", null, acl, ephemeralOwner, parentCVersion, zxid, time);
				} catch (NoNodeException | NodeExistsException e) {
					e.printStackTrace();
				}
			}					
		}
		
		@Test
		public void createNodeTest() {
			int startContainers = 0;
			int startTtls = 0;
			int startEphemerals = 0;
			int startNodes = 0;
			
			try {
				startContainers = dataTree.getContainers().size();
				startTtls = dataTree.getTtls().size();
				startEphemerals = dataTree.getEphemeralsCount();
				startNodes = dataTree.getNodeCount();
				
				dataTree.createNode(path, data, acl, ephemeralOwner, parentCVersion, zxid, time, outputStat);
				
				boolean extendedTypesEnabled = System.getProperty("zookeeper.extendedTypesEnabled") == "true";
				boolean emulate353TTLNodes = System.getProperty("zookeeper.emulate353TTLNodes") == "true";
				
				if (ephemeralOwner == Long.MIN_VALUE)
					Assert.assertEquals(expectedResult, Integer.toString(dataTree.getContainers().size()-startContainers));
				else if (extendedTypesEnabled && emulate353TTLNodes && ephemeralOwner < 0)
					Assert.assertEquals(expectedResult, Integer.toString(dataTree.getTtls().size()-startTtls));
				else if (ephemeralOwner != 0)
					Assert.assertEquals(expectedResult, Integer.toString(dataTree.getEphemeralsCount()-startEphemerals));
				else
					Assert.assertEquals(expectedResult, Integer.toString(dataTree.getNodeCount()-startNodes));
			} catch (NoNodeException e) {
				Assert.assertEquals(expectedResult, e.getMessage());
			} catch (NodeExistsException e) {
				Assert.assertEquals(expectedResult, e.getMessage());
			} catch (NullPointerException e) {
				Assert.assertEquals(expectedResult, e.getMessage());
			} catch (StringIndexOutOfBoundsException e) {
				Assert.assertEquals(expectedResult, e.getMessage());
			}
		}
	}
	
	@RunWith(Parameterized.class)
	public static class SetDataTest {
		
		private String path;
		private byte[] data;
		private int version;
		private long zxid;
		private long time;
		private String expectedResult;

		private DataTree tree;
		
		@Parameters
		public static Collection<Object[]> data() {
			
			byte[] empty = { };
			
	        return Arrays.asList(new Object[][] {
	        	{ null, null, null, 0, 0, 0, "null" },
	        	{ "/notexists", null, "ciao".getBytes(), 0, 0, 0, "NoNode" },
	        	{ "/node", null, null, 0, 0, 0, "null" },
	        	{ "/node", null, empty, 0, 0, 0, "" },
	        	{ "/node", null, "ciao".getBytes(), 0, 0, 0, "ciao" },
	        	{ "/node", null, "ciao".getBytes(), -1, -1, 1, "ciao" },
	        	{ "/node", null, "ciao".getBytes(), 1, 1, -1, "ciao" },
	        	{ "/node", "old".getBytes(), "ciao".getBytes(), 1, 1, -1, "ciao" }
	        	
	        });
	    }
		
		public SetDataTest(String path, byte[] originData, byte[] data, int version, long zxid, long time, String expectedResult) {
			configure(path, originData, data, version, zxid, time, expectedResult);
		}
		
		public void configure(String path, byte[] originData, byte[] data, int version, long zxid, long time, String expectedResult) {
			// create node
			tree = new DataTree();
			try {
				tree.createNode("/node", originData, null, 0, 0, 0, 0);
			} catch (NoNodeException | NodeExistsException e) {
				e.printStackTrace();
			}
			
			this.path = path;
			this.data = data;
			this.version = version;
			this.zxid = zxid;
			this.time = time;
			this.expectedResult = expectedResult;
		}
		
		@Test
		public void setDataTest() {
			try {
				tree.setData(path, data, version, zxid, time);
				Assert.assertEquals(expectedResult, new String(tree.getData(path, new Stat(), null), StandardCharsets.UTF_8));
			} catch (NoNodeException e) {
				Assert.assertTrue(e.getMessage().contains(expectedResult));
			} catch (NullPointerException e) {
				Assert.assertEquals(null, e.getMessage());
			}
			
		}
		
	}
}