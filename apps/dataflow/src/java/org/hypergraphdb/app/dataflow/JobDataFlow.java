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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import org.hypergraphdb.handle.UUIDHandleFactory;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Mapping;
import org.hypergraphdb.util.Pair;

/**
 * 
 * <p>
 * A <code>DistributedDataFlow</code> represents a distributed data flow network. That is,
 * a data flow network where processing nodes are spread across physically separate
 * machines. There's a distinction b/w the underlying logical network that defines the
 * software program implemented by the data flow and the actual runtime network resulting
 * from load-balancing and parallelization of specific nodes.  
 * </p>
 *
 * @author Borislav Iordanov
 *
 * @param <ContextType>
 */
public class JobDataFlow<ContextType> extends DataFlowNetwork<ContextType>
{
    public static final String EXCEPTION_CHANNEL_ID = "EXCEPTION_CHANNEL";
    public static final String JOB_CHANNEL_ID = "JOB_CHANNEL";
    
    public static final DistributedException EOF_EXCEPTION = 
        new DistributedException(UUIDHandleFactory.I.nullHandle(), null);

    private ContextJobAdapter<ContextType> jobAdapter;
    private transient ChannelManager channelManager;
    
    transient List<JobListener<ContextType>> listeners = 
        Collections.synchronizedList(new ArrayList<JobListener<ContextType>>());
    
    // For each job that is currently running, keep track of the number of
    // terminal (i.e. "ending") processor that haven't finished yet. This
    // allows us to detect when the job has completed. 
    transient Map<Job, Integer> activeProcessors = 
        Collections.synchronizedMap(new HashMap<Job, Integer>());
    
    transient Map<Job, List<DistributedException>> jobExceptions =
    	Collections.synchronizedMap(new HashMap<Job,List<DistributedException>>());
    
    transient JobProcessorListener jobProcessorlistener = new JobProcessorListener();
    
    public static <CT> 
        JobDataFlow<CT> make(DataFlowNetwork<CT> network,
                                 ContextJobAdapter<CT> adapter,
                                 Job EOF_JOB)
    {
        JobDataFlow<CT> result = new JobDataFlow<CT>();
        result.setJobAdapter(adapter);
        for (Channel<?> channel : network.getChannels())
            result.addChannel(new Channel<Object>(channel.getId(), channel.getEOS(), channel.getCapacity()));
        result.addChannel(new Channel<Throwable>(EXCEPTION_CHANNEL_ID, EOF_EXCEPTION));
        result.addChannel(new Channel<Job>(JOB_CHANNEL_ID, EOF_JOB));

        for (Processor<CT> node : network.getNodes())
        {
            Set<String> ins = new HashSet<String>();
            Set<String> outs = new HashSet<String>();
            ins.add(JOB_CHANNEL_ID);
            outs.add(EXCEPTION_CHANNEL_ID);
            ins.addAll(network.getInputs(node));
            outs.addAll(network.getOutputs(node));
            result.addNode(new JobProcessor<CT>(result, node), ins, outs);
        }
        result.setContext(network.getContext());
        return result;
    }

    @SuppressWarnings("unchecked")
    public static <CT>
        JobDataFlow<CT> clone(JobDataFlow<CT> network)
    {
        try
        {        
            JobDataFlow<CT> result = new JobDataFlow<CT>();
            result.setJobAdapter(network.getJobAdapter());
            for (Channel<?> channel : network.getChannels())
                result.addChannel(new Channel<Object>(channel.getId(), 
                                                      channel.getEOS(), 
                                                      channel.getCapacity()));
            Mapping<Pair<Object, String>, Boolean> propertyFilter = 
                new Mapping<Pair<Object, String>, Boolean>() {
                    public Boolean eval(Pair<Object, String> p)
                    {
                        return !(p.getFirst() instanceof Processor) ||
                               !p.getSecond().equals("network");
                    }
                
            };
            
            for (Processor<CT> node : network.getNodes())
                result.addNode((Processor<CT>)HGUtils.cloneObject(node, propertyFilter), 
                               network.getInputs(node), 
                               network.getOutputs(node));
            return result;
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }
    
    @Override
    public Future<Boolean> start()
    {        
        for (Processor<ContextType> p : getNodes())
        {
            if (!(p instanceof JobProcessor<?>))
                continue;
            JobProcessor<ContextType> jp = (JobProcessor<ContextType>)p;
            // Avoid duplicates when multiple calls to 'start' are made. 
            jp.removeJobListener(jobProcessorlistener); 
            jp.addJobListener(jobProcessorlistener);
        }
        return super.start();
    }
    
    public void disconnect(JobProcessor<ContextType> processor) throws InterruptedException
    {        
        processor.disconnect();
        removeNode(processor);
    }
    
    /**
     * <p>
     * Connected the passed in processor to the network. This is intended to be
     * use at runtime while the network is paused and/or there are no current
     * jobs being processed. It is possible to specify a list of jobs that
     * will be attached specifically to this processor. Such pending jobs
     * have presumably already been submitted to the rest of the network
     * nodes. The JobProcessor will first deal with them before reading
     * from the JOB_CHANNEL.
     * </p>
     * 
     * @param processor The job processor - if it is not an instance of 
     * <code>JobProcessor</code>, it will be wrapped into one.
     * @param inputChannelIds 
     * @param outputChannelIds
     * @param pendingJobs
     */
    public void connect(Processor<ContextType> processor,
                        Set<String> inputChannelIds,
                        Set<String> outputChannelIds,
                        Collection<Job> pendingJobs)
    {
        
        if (processor instanceof JobProcessor<?>)
            processor = ((JobProcessor<ContextType>)processor).processor; 
        JobProcessor<ContextType> newP = 
                new JobProcessor<ContextType>(this, processor);
        if (pendingJobs != null)
        {
            newP.pendingJobs = new LinkedList<Job>();
            newP.pendingJobs.addAll(pendingJobs);
        }
        Set<String> ins = new HashSet<String>();
        Set<String> outs = new HashSet<String>();
        ins.add(JOB_CHANNEL_ID);
        ins.addAll(inputChannelIds);
        outs.add(EXCEPTION_CHANNEL_ID);
        outs.addAll(outputChannelIds);        
        addNode(newP, ins, outs);
        startNode(newP);
    }
    
    /**
     * <p>
     * Submit a job to the network - equivalent to writing to the job channel.
     * </p>
     * 
     * @param job The job to be written to the job channel.
     * @throws InterruptedException
     */
    public void submitJob(Job job) throws InterruptedException
    {
        getJobChannel().put(job);
    }
    
    public static boolean isChannelJobSpecific(String channelId)
    {
        return !channelId.contains(JobDataFlow.JOB_CHANNEL_ID)
            && !channelId.contains(JobDataFlow.EXCEPTION_CHANNEL_ID);
    }
    
    public Channel<Throwable> getExceptionChannel()
    {
        return getChannel(EXCEPTION_CHANNEL_ID);
    }
        
    public Channel<Job> getJobChannel()
    {
        return getChannel(JOB_CHANNEL_ID);
    }
    
    public ContextJobAdapter<ContextType> getJobAdapter()
    {
        return jobAdapter;
    }

    public void setJobAdapter(ContextJobAdapter<ContextType> jobAdapter)
    {
        this.jobAdapter = jobAdapter;
    }

    public ChannelManager getChannelManager()
    {        
        return channelManager;
    }

    public void setChannelManager(ChannelManager channelManager)
    {
        this.channelManager = channelManager;
    }    
    
    /**
     * <p>
     * Return the set of all processor that don't read from any of
     * the network channels.
     * </p>
     */
    public Set<JobProcessor<ContextType>> findStartingNodes()
    {
        HashSet<JobProcessor<ContextType>> result = new HashSet<JobProcessor<ContextType>>();
        for (Processor<ContextType> p : getNodes())
        {
            if (!(p instanceof JobProcessor<?>))
                continue;
            
            Set<String> inputs = getInputs(p);
            if (inputs.isEmpty() || 
                inputs.size() == 1 && inputs.iterator().next().equals(JOB_CHANNEL_ID));
                result.add((JobProcessor<ContextType>)p);
        }
        return result;
    }
    
    /**
     * <p>
     * Return the set of all processor that don't write to any of 
     * the network channels.
     * </p>
     */
    public Set<JobProcessor<ContextType>> findEndingNodes()
    {
        HashSet<JobProcessor<ContextType>> result = new HashSet<JobProcessor<ContextType>>();
        for (Processor<ContextType> p : getNodes())
        {
            if (!(p instanceof JobProcessor<?>))
                continue;
            
            Set<String> outputs = getOutputs(p);
            if (outputs.isEmpty() || 
                outputs.size() == 1 && outputs.iterator().next().equals(EXCEPTION_CHANNEL_ID));
                result.add((JobProcessor<ContextType>)p);
        }
        return result;
    }
    
    public void addJobListener(JobListener<ContextType> listener)
    {
        listeners.add(listener);
    }
    
    public void removeJobListener(JobListener<ContextType> listener)
    {
        listeners.remove(listener);
    }
    
    private class JobProcessorListener implements JobListener<ContextType>
    {
        public void startJob(Job job, ContextType ctx, Object source)
        {
            synchronized (activeProcessors)
            {
                Integer count = activeProcessors.get(job);
                if (count == null)
                {
                    activeProcessors.put(job, findEndingNodes().size());
                    for (JobListener<ContextType> l : listeners)
                        l.startJob(job, ctx, JobDataFlow.this);
                }
            }             
        }
        
        public void endJob(Job job, 
        				   ContextType ctx, 
        				   Object source, 
        				   List<DistributedException> exL)
        {
			synchronized (jobExceptions)
			{
				List<DistributedException> L = jobExceptions.get(job);
				if (L == null)
				{
					L = new ArrayList<DistributedException>();
					jobExceptions.put(job, L);
				}
				L.addAll(exL);
			}        	
            synchronized (activeProcessors)
            {                
                Integer count = activeProcessors.remove(job);
                int newCount = count - 1;
                System.out.println("Job ended by " + source + " remaining " + newCount);
                if (newCount == 0)
                {
                	List<DistributedException> exceptionList = jobExceptions.remove(job);
                    for (JobListener<ContextType> l : listeners)
                        l.endJob(job, ctx, JobDataFlow.this, exceptionList);
                    channelManager.notifyJobDone(job);
                }
                else
                    activeProcessors.put(job, newCount);
            }
        }
    }
}
