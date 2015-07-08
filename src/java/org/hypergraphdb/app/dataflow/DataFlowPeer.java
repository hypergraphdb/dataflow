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

import java.util.ArrayList;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.HyperGraphPeer;

/**
 * 
 * <p>
 * Represents a peer participant in a distributed data flow. A peer participant
 * is bound to a single <code>DistributedDataFlow</code> network and it can
 * act as the master node that initiates processing or a slave node that joins
 * and potentially leaves an existing network.
 * </p>
 * 
 * @author Borislav Iordanov
 * 
 */
public class DataFlowPeer<ContextType>
{
    private boolean master = false;
    private HyperGraphPeer hgpeer;
    private HGHandle networkHandle;
    private ChannelPeer<ContextType> channelPeer;
    JobDataFlow<ContextType> network;
    
    /**
     * A P2P dataflow network is stored by storing the DataFlowNetwork object as an
     * atom, each processor as an atom and each channel as a HGBergeLink between 
     * processors (see ChannelLink class). In addition, plain links of the form 
     * (networkHandle, processorHandle, peerIdHandle) define which processor is
     * ran at which peer.  
     */
    @SuppressWarnings("unchecked")
    private void loadJobNetwork()
    {
        HyperGraph graph = hgpeer.getGraph();
        network = graph.get(networkHandle);
        if (network == null)
            return;
        
        network.clearAll();

        List<HGLink> processors = hg.getAll(graph,
                                            hg.orderedLink(networkHandle,
                                                           hg.anyHandle(),
                                                           hg.anyHandle()));

        channelPeer = new ChannelPeer<ContextType>(hgpeer, network);
        network.setChannelManager(channelPeer);
        
        // First pass: just collect local "logical" channels and add them to the
        // network. We only include channels that are connected to processors
        // running locally.
        for (HGLink l : processors)
        {
            HGHandle peerId = l.getTargetAt(2);
            if (!peerId.equals(hgpeer.getIdentity().getId()))
                continue;
            List<ChannelLink<Job>> channels = hg.getAll(graph,
                                                            hg.and(hg.type(ChannelLink.class),
                                                                   hg.incident(l.getTargetAt(1))));
            for (ChannelLink<Job> ch : channels)
                network.addChannel(ch.getChannel());
        }

        // Second pass: add processors to the network and add channel peers to
        // for local channels
        for (HGLink l : processors)
        {
            HGHandle peerId = l.getTargetAt(2);
            HGHandle processorHandle = l.getTargetAt(1);
            JobProcessor<ContextType> processor = graph.get(processorHandle);
            List<ChannelLink<Job>> channels = hg.getAll(graph,
                                                            hg.and(hg.type(ChannelLink.class),
                                                                   hg.incident(l.getTargetAt(1))));

            if (peerId.equals(hgpeer.getIdentity().getId())) // is it a local processor?
            {
                ArrayList<String> inputs = new ArrayList<String>();
                ArrayList<String> outputs = new ArrayList<String>();
                for (ChannelLink<Job> ch : channels)
                {
                    if (ch.getHead().contains(processorHandle))
                        inputs.add(ch.getChannel().getId());
                    else
                        outputs.add(ch.getChannel().getId());
                }                
                processor.setNetwork(network);
                network.addNode(processor, 
                                inputs.toArray(new String[0]), 
                                outputs.toArray(new String[0]));
            }
            else
            {
                for (ChannelLink<Job> ch : channels)
                    if (network.getChannel(ch.getChannel().getId()) != null)
                        channelPeer.addChannelPeer(ch.getChannel().getId(),
                                                   (HGPeerIdentity) graph.get(peerId));
            }
        }
        if (network.getChannel(JobDataFlow.JOB_CHANNEL_ID) != null)
            network.addNode(new JobTrackProcessor(), 
                            new String[] { JobDataFlow.JOB_CHANNEL_ID}, 
                            new String[]{});
        channelPeer.initNetworking();
    }

    public DataFlowPeer()
    {
    }

    public DataFlowPeer(HyperGraphPeer hgpeer, HGHandle networkHandle)
    {
        this.hgpeer = hgpeer;
        this.networkHandle = networkHandle;        
    }

    public JobDataFlow<ContextType> getNetwork()
    {
        if (network == null)
            loadJobNetwork();            
        return network;
    }
    
    /**
     * <p>
     * Start processing of the network identified by the <code>HGHandle</code>
     * argument. Calling this method will attempt to make this peer into a
     * <em>master</em> peer for the execution of the network. If the peer
     * finds an already existing master that is running, it will exit with an
     * exception.
     * </p>
     * 
     * @param network
     *            Handle to a valid <code>DistributedFlowNetwork</code>
     *            instance.
     */
    public Future<?> start()
    {
        assert hgpeer != null;
        return getNetwork().start();
    }

    /**
     * <p>
     * Shutdown a master peer explicitly and trigger a graceful shutdown of the
     * whole network.
     * </p>
     */
    public void shutdown()
    {
        channelPeer.shutdownNetworking();    	
        if (network == null)
            return;
        if (!network.isIdle())
            network.shutdown();
        int attempts = 100;
        while (!network.isIdle() && attempts-- > 0)
        {
//        	DU.log.info("Forcing shutdown of network " + network);
        	network.kill();
        	try { Thread.sleep(1000); } catch (InterruptedException ex) { }
        }
        network = null;
        channelPeer = null;
    }
    
    public void submitJob(Job job) throws InterruptedException
    {
        assert network != null : new RuntimeException("Network not started.");
        if (!master)
            throw new RuntimeException("Can't submit jobs to this peer because it's not a master peer.");
        // Make sure we are able to create job context.
        try
        {
        	network.getJobAdapter().adapt(network.getContext(), job);
        }
        catch (Throwable t)
        {
        	throw new DataFlowException("Failed to create context for job " + job, t);
        }
        if (job.getHandle() == null)
        {
            HGPersistentHandle h = hgpeer.getGraph().getHandleFactory().makeHandle(); 
            job.setHandle(h);
            hgpeer.getGraph().define(h, job);
        }
        network.submitJob(job);        
    }
    
    // Currently leads to deadlock when a processor can't complete its
    // job because other processor are paused. This happens when a processor
    // is ahead with its job processing compared to others.
    //
    // The solution is the pause right after the last job that was started.
    // Because some processors are much faster than other, it is actually
    // wise the limit channel capacity across the board so that one doesn't
    // end up with 100 jobs being done by processor A while processor B is
    // stuck on the first job only.
    public void pause() throws InterruptedException, ExecutionException
    {
        network.getJobChannel().addCap();
        // NOTE: this could be moved to the JobDataFlow class...
        
        // First tell all JobProcessor to pause right after they complete
        // their current job.
        for (Processor<ContextType> proc : network.getNodes())
        {
            if (proc instanceof JobProcessor<?>)
                ((JobProcessor<?>) proc).pause();
        }
        // Then, wait for all JobProcessor to enter a "paused" state.
        // This may take a while since a single job processing for a 
        // processor may last an arbitrary amount of time.
        for (Processor<ContextType> proc : network.getNodes())
        {
            if (proc instanceof JobProcessor<?>)
                ((JobProcessor<?>) proc).waitPaused(true);
        }        
        
        // No need to do anything about the JobTrackingProcessor which is not
        // even part of the job network. In fact, this processor can continue
        // running without a problem.
    }
    
    public void resume()
    {
        network.getJobChannel().removeCap();
        for (Processor<ContextType> proc : network.getNodes())
        {
            if (proc instanceof JobProcessor<?>)
                ((JobProcessor<?>) proc).resume();
        }        
    }
    
    public HyperGraphPeer getHgpeer()
    {
        return hgpeer;
    }

    public void setHgpeer(HyperGraphPeer hgpeer)
    {
        this.hgpeer = hgpeer;
    }

    public HGHandle getNetworkHandle()
    {
        return networkHandle;
    }

    public void setNetworkHandle(HGHandle networkHandle)
    {
        this.networkHandle = networkHandle;
    }
        
    public boolean isMaster()
    {
        return master;
    }

    public void setMaster(boolean master)
    {
        this.master = master;
    }
}
