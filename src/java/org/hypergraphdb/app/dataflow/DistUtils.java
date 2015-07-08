/*******************************************************************************
 * Copyright (c) 2011 Kobrix Software, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser Public License v2.1
 * which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/old-licenses/gpl-2.0.html
 * 
 * Contributors:
 *    Borislav Iordanov, Murilo Saraiva de Queiroz - initial API and implementation
 ******************************************************************************/
package org.hypergraphdb.app.dataflow;

import java.util.*;
import java.util.concurrent.Callable;

import org.hypergraphdb.*;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.PrivatePeerIdentity;

@SuppressWarnings("unchecked")
public class DistUtils
{     
	public static <T> HGHandle saveDataFlowNetwork(final HyperGraph graph, 
	                                               final DataFlowNetwork<T> network,
	                                               final HGHandle peerIdentity)
	{
	    return graph.getTransactionManager().ensureTransaction(new Callable<HGHandle>() {	        
	    public HGHandle call()
	    {
	    HGHandle networkHandle = graph.getHandle(network);
		if (networkHandle == null)
			networkHandle = graph.add(network);
		else
			throw new RuntimeException("Network with handle " + networkHandle + " already saved.");
		
        Map<Object, Set<HGHandle>> channelInputs = new HashMap<Object, Set<HGHandle>>();
        Map<Object, Set<HGHandle>> channelOutputs = new HashMap<Object, Set<HGHandle>>();
		
		for (Channel<?> ch : network.getChannels())
		{
		    channelInputs.put(ch.getId(), new HashSet<HGHandle>());
		    channelOutputs.put(ch.getId(), new HashSet<HGHandle>());
		}
		
		for (Processor<T> node : network.getNodes())
		{
		    HGHandle h = graph.add(node);
		    if (peerIdentity == null)
		        graph.add(new HGPlainLink(networkHandle, h));
		    else
		        graph.add(new HGPlainLink(networkHandle, h, peerIdentity));
		    for (Object chId : network.getInputs(node))
		        channelInputs.get(chId).add(h);
            for (Object chId : network.getOutputs(node))
                channelOutputs.get(chId).add(h);		    
		}
		
        for (Channel<?> ch : network.getChannels())
            graph.add(new ChannelLink<Object>((Channel<Object>)ch, 
                                      channelInputs.get(ch.getId()),
                                      channelOutputs.get(ch.getId())));
        
        return networkHandle;
	    }});
	}
	
	public static void removeNetwork(final HyperGraph graph, final HGHandle networkHandle)
	{
	    if (networkHandle == null)
	        throw new NullPointerException("null network handle passed to DistUtils.removeNetwork.");
	    graph.getTransactionManager().ensureTransaction(new Callable<Object>() {	        
	    public Object call()
	    {
        List<HGHandle> processors = hg.findAll(graph, hg.apply(hg.targetAt(graph, 1),
                                                               hg.orderedLink(networkHandle, 
                                                                              hg.anyHandle())));
        for (HGHandle h : processors)
            graph.remove(h, false);
        graph.remove(networkHandle);
        return null;
	    }});
	}
	
	/**
	 * <p>
	 * Load a data-flow network stored in the passed in <code>HyperGraph</code> instance.
	 * </p>
	 * 
	 * @param <T>
	 * @param graph
	 * @param networkHandle
	 * @return
	 */
    public static <T> DataFlowNetwork<T> loadNetwork(HyperGraph graph, HGHandle networkHandle)
    {        
        DataFlowNetwork<T> result = graph.get(networkHandle);
        if (result == null)
            return null;
        
        // We want to effectively reload a fresh copy of the network even if modified in
        // memory.
        result.clearAll();         
        
        List<HGHandle> processors = hg.findAll(graph, hg.apply(hg.targetAt(graph, 1),
                                                               hg.orderedLink(networkHandle, 
                                                                              hg.anyHandle())));
        for (HGHandle h : processors)
        {
            List<ChannelLink<?>> L = hg.getAll(graph, hg.and(hg.type(ChannelLink.class), 
                                                                 hg.incident(h)));
            for (ChannelLink<?> x : L)
                result.addChannel(x.getChannel());
        }
        
        for (HGHandle h : processors)
        {
            List<ChannelLink<?>> L = hg.getAll(graph, hg.and(hg.type(ChannelLink.class), 
                                                             hg.incident(h)));
            List<Object> inputs = new ArrayList<Object>();
            List<Object> outputs = new ArrayList<Object>();
            for (ChannelLink<?> x : L)
            {
                if (x.getHead().contains(h))
                    inputs.add(x.getChannel().getId());
                else
                    outputs.add(x.getChannel().getId());
            }            
            Processor<T> processor = graph.get(h); 
            if (processor instanceof JobProcessor)
                ((JobProcessor)processor).setNetwork((JobDataFlow)result);
            result.addNode(processor, 
                           inputs.toArray(new String[0]), 
                           outputs.toArray(new String[0]));
        }            
        return result;
    }
    
    /**
     * <p>
     * Mark the given processor as running within the network at the given peer. This
     * is stated as a HGPlainLink[network, processor, peer]. If such a link already
     * exists, it is removed. If more than one such links exist, an exception is thrown.
     * </p>  
     */
    public static void setOwningPeer(final HyperGraph graph, 
                                     final HGHandle network, 
                                     final HGHandle processor, 
                                     final HGHandle peer)
    {
        graph.getTransactionManager().transact(new Callable<Object>() {
            public Object call()
            {
                List<HGHandle> L = hg.findAll(graph, 
                                              hg.and(hg.type(HGPlainLink.class),
                                                     hg.orderedLink(network, processor, hg.anyHandle())));
                // There should be exactly one element here so we assert the assumption
                if (L.size() > 1)
                    throw new RuntimeException("Processor " + processor + " declared at more than one peer location " + L);
                else if (!L.isEmpty())
                    graph.remove(L.get(0));
                // Mark the processor as running locally.
                graph.add(new HGPlainLink(network, processor, peer));        
                return null;
            }                       
        });
    }
    
    public static HGPeerIdentity getOwningPeer(final HyperGraph graph, final HGHandle network, final HGHandle processor)
    {
        HGHandle h= hg.findOne(graph, hg.and(hg.type(HGPlainLink.class), 
                                             hg.orderedLink(network, processor, hg.anyHandle())));
        if (h == null)
            return null;
        HGPlainLink pl = graph.get(h);
        PrivatePeerIdentity pid = graph.get(pl.getTargetAt(2));
        return pid.makePublicIdentity();
    }
}
