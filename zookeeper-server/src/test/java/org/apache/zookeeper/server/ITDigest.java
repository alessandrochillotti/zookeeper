package org.apache.zookeeper.server;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.zookeeper.data.StatPersisted;
import org.junit.Assert;
import org.junit.Test;

public class ITDigest {
	
	NodeHashMapImpl nodeHashMap;
	DigestCalculator digestCalculator;
	DataNode node;
	
	public ITDigest() {

	}
	
	@Test
	public void commHashMapDigestCalculator() {
		digestCalculator = mock(DigestCalculator.class);
		nodeHashMap = new NodeHashMapImpl(digestCalculator);
		node = new DataNode();
		
		nodeHashMap.put("/node", node);
		
		verify(digestCalculator, times(1)).calculateDigest("/node", node);
	}
	
	@Test
	public void commDigestCalculatorDataNode() {
		digestCalculator = new DigestCalculator();
		node = mock(DataNode.class);
		node.stat = mock(StatPersisted.class);
		
		long digest = digestCalculator.calculateDigest("/node", node.getData(), node.stat);
		digestCalculator.calculateDigest("/node", node);
		
		verify(node, times(1)).setDigest(digest);
	}
	
	@Test
	public void assertDigestCalculatorDataNode() {
		digestCalculator = new DigestCalculator();
		node = new DataNode("prova".getBytes(), Long.valueOf(0), new StatPersisted());
		
		digestCalculator.calculateDigest("/node", node);
		
		Assert.assertEquals(node.getDigest(), digestCalculator.calculateDigest("/node", node.getData(), node.stat));
	}
	
	@Test
	public void assertHashMapDigestCalculator() {
		digestCalculator = new DigestCalculator();
		nodeHashMap = new NodeHashMapImpl(digestCalculator);
		node = new DataNode("prova".getBytes(), Long.valueOf(0), new StatPersisted());
		
		long startHash = nodeHashMap.getDigest();
		
		nodeHashMap.put("/node", node);
		
		Assert.assertEquals(startHash + digestCalculator.calculateDigest("/node", node.getData(), node.stat), nodeHashMap.getDigest());
		Assert.assertEquals(digestCalculator.calculateDigest("/node", node.getData(), node.stat), node.getDigest());
	}
}
