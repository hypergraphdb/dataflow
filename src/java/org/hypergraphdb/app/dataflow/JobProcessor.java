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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.hypergraphdb.app.dataflow.monitor.AvgAccumulator;
import org.hypergraphdb.app.dataflow.monitor.MonitoringInputPort;
import org.hypergraphdb.app.dataflow.monitor.MonitoringOutputPort;
import org.hypergraphdb.annotation.HGIgnore;

public class JobProcessor<ContextType> implements Processor<ContextType>
{
    transient Object pauseLock = new Object();
    transient Object pauseRequestLock = new Object();
    volatile transient boolean pauseRequested = false;
    volatile transient boolean paused = false;
    volatile transient Object disconnectSignal = null;
    volatile transient Job currentJob = null;
    transient List<JobListener<ContextType>> listeners = 
        Collections.synchronizedList(new ArrayList<JobListener<ContextType>>());
    
    Processor<ContextType> processor = null;
    
    // Store unprocessed jobs from the JOB_CHANNEL InputPort buffer when
    // this processor is explicitly disconnected.
    LinkedList<Job> pendingJobs = null;
    
    // private transient Thread thisThread = null;

    @HGIgnore
    private JobDataFlow<ContextType> network;

    private boolean monitoringOn = true;
    private Map<String, AvgAccumulator> flowRates = new HashMap<String, AvgAccumulator>();

    private AvgAccumulator getAvgAccumulator(String id)
    {
        AvgAccumulator acc = flowRates.get(id);
        if (acc == null)
        {
            acc = new AvgAccumulator();
            flowRates.put(id, acc);
        }
        return acc;
    }

    public JobProcessor()
    {
    }

    public JobProcessor(JobDataFlow<ContextType> network,
            Processor<ContextType> processor)
    {
        this.network = network;
        this.processor = processor;
    }

    public String getName()
    {
        return processor.getName();
    }

    public String toString()
    {
        return "JOB[" + getName() + "]";
    }
    
    public void disconnect() throws InterruptedException
    {
        disconnectSignal = new Object();
        synchronized (disconnectSignal)
        {
            disconnectSignal.wait();
        }
    }

    /**
     * <p>
     * Cause this processor to stop reading and processing jobs until
     * the {@link resume} method is called. The processor might
     * still be doing a job and the pause action will not be in effect
     * until the current job is finished. This method returns right away
     * regardless of whether a job is being processed at the moment. To
     * ensure that a job has completed and that the processed in 
     * indeed in a paused state, called <code>waitPaused(true)</code>.
     * </p>
     */
    public void pause()
    {
        synchronized (pauseLock)
        {
            pauseRequested = true;
        }
    }
    
    /**
     * <p>
     * Resume processing jobs after a call to {@link pause}. 
     * </p>
     */
    public void resume()
    {
        synchronized (pauseLock)
        {
            pauseRequested = false; // clear any pending pause request
            paused = false;            
            pauseLock.notifyAll();
        }
    }
    
    /**
     * <p>Return <code>true</code> if this JobProcessor is currently paused 
     * and <code>false</code> otherwise.
     */
    public boolean isPaused()
    {
        synchronized (pauseLock)
        {
            return paused;
        }
    }
    
    /**
     * <p>
     * Return the job currently being processed by this processor 
     * or <code>null</code> is no job is currently being processed.
     * This is just for monitoring purposes - there's no guarantee 
     * that once the method has returned, the same job will still be
     * in process. 
     * </p>
     */
    public Job getCurrentJob()
    {
        return currentJob;
    }
    
    /**
     * <p>
     * Wait until the JobProcessor becomes paused (if the <code>paused</code>
     * parameter is <code>true</code>) or until it becomes active
     * (if the <code>paused</code> parameter is <code>false</code>). 
     * </p>
     * 
     * @param paused
     * @throws InterruptedException
     */
    public void waitPaused(boolean paused) throws InterruptedException
    {
        if (paused)
            synchronized (pauseRequestLock)
            {
                while (!this.paused) // wait for pause request to complete
                    pauseRequestLock.wait(); 
            }
        else
            synchronized (pauseLock)
            {
                while (this.paused)
                    pauseLock.wait();
            }
    }
    
    @SuppressWarnings("unchecked")
    public void process(ContextType ctx, Ports ports) throws InterruptedException
    {
        InputPort<Job> jobIn = ports.getInput(network.getJobChannel().getId());        
        // thisThread = Thread.currentThread();
        while (disconnectSignal == null)
        {
            synchronized (pauseLock)
            {            
                if (pauseRequested)
                {
                    paused = true;                        
                    synchronized (pauseRequestLock)
                    {    
                        pauseRequested = false;                    
                        pauseRequestLock.notifyAll();
                    }
                    while (paused)
                        pauseLock.wait();
                }
            }      
            
            Job job = null;
            if (pendingJobs != null && !pendingJobs.isEmpty())
                job = pendingJobs.removeFirst();
            else
            {
                // We don't use jobIn.take here so that we can periodically check
                // whether we're being disconnected.
                job = jobIn.poll();
                if (job == null)                
                    try { Thread.sleep(1000); continue; }
                    catch (InterruptedException ex) { break; }
            }
            if (jobIn.isEOS(job))
                break;

            ContextType CC = ctx;
            if (network.getJobAdapter() != null)
            {
            	try
            	{
            		CC = network.getJobAdapter().adapt(ctx, job);
            	}
            	catch (Throwable t)
            	{
//            		DU.log.error("Failed to create context for job " + job
//            				+ ", skipping - may lead to deadlock if other JobProcessor instances succeed.", t);
            		continue;
            	}
            }
            
            currentJob = job;
            for (JobListener<ContextType> l : listeners.toArray(new JobListener[0]))
                    l.startJob(currentJob, CC, this);
            
            // Create separate ports and channels for this job:
            Ports innerPorts = new Ports();
            for (InputPort<?> p : ports.getInputPorts())
                if (!JobDataFlow.isChannelJobSpecific(p.getChannel().getId()))
                    continue;
                else
                {
                    JobChannel<?> ch = network.getChannelManager()
                            .getJobChannel(network, p.getChannel(), job);
                    InputPort<?> port = ch.newInputPort();
                    if (monitoringOn)
                        port = new MonitoringInputPort(port, 
                                                       getAvgAccumulator(p.getChannel().getId()));
                    if (processor instanceof LoadBalancedNode || 
                    	processor instanceof InputSplitter ||
                    	processor instanceof OutputCombiner)
                    	innerPorts.addPort(port);
                    else // make sure that is a LoadBalancedNode wraps this JobProcessor 
                    	 // we use the original, "unbalanced" channel ID
	                    innerPorts.getInputMap().put(
	                    		LoadBalancer.originalChannelId(port.getChannel().getId()), 
	                    		port);
                }
            for (OutputPort<?> p : ports.getOutputPorts())
                if (!JobDataFlow.isChannelJobSpecific(p.getChannel().getId()))
                    continue;
                else
                {
                    JobChannel<?> ch = network.getChannelManager()
                            .getJobChannel(network, p.getChannel(), job);
                    OutputPort<?> port = ch.newOutputPort();
                    if (monitoringOn)
                        port = new MonitoringOutputPort(port, 
                                                        getAvgAccumulator(p.getChannel().getId()));
                    if (processor instanceof LoadBalancedNode || 
                       	processor instanceof InputSplitter ||
                       	processor instanceof OutputCombiner)
                    	innerPorts.addPort(port);
                    else
	                    innerPorts.getOutputMap().put(
	                    		LoadBalancer.originalChannelId(port.getChannel().getId()), 
	                    		port);
                }

            DistributedException ex = null;
            try
            {
                innerPorts.openAll();
                processor.process(CC, innerPorts);
            }
            catch (Throwable t)
            {
                System.err.println("Processor " + getName()
                        + " bailed out, stack trace follows...");
                t.printStackTrace();
                ex = new DistributedException(this, currentJob, t.getMessage(), t);
                network.getExceptionChannel().put(ex);
            }
            finally
            {
                // System.out.println("Done with " + processor.getName());
                innerPorts.closeAll();
                ArrayList<DistributedException> exL = new ArrayList<DistributedException>();
                if (ex != null)
                	exL.add(ex);
                for (JobListener<ContextType> l : listeners.toArray(new JobListener[0]))
                    l.endJob(currentJob, CC, this, exL);                
                currentJob = null;                
            }
        }
        if (disconnectSignal != null)
        {
            pendingJobs = new LinkedList<Job>();
            for (Job pending = jobIn.poll(); 
                 pending != null && !jobIn.isEOS(pending); pending = jobIn.poll())
                pendingJobs.add(pending);
            synchronized (disconnectSignal)
            {
                disconnectSignal.notifyAll();
            }
        }
    }

    public Processor<ContextType> getProcessor()
    {
        return processor;
    }

    public void setProcessor(Processor<ContextType> processor)
    {
        this.processor = processor;
    }

    public JobDataFlow<ContextType> getNetwork()
    {
        return network;
    }

    public void setNetwork(JobDataFlow<ContextType> network)
    {
        this.network = network;
    }
    
    /**
     * <p>
     * Return the list of un-processed jobs that were submitted to this particular
     * processor, but were never started because the processor was explicitly 
     * disconnected by a call to <code>disconnect</code>.
     * </p>
     */
    public LinkedList<Job> getPendingJobs()
    {
        return pendingJobs;
    }
    
    /**
     * <p>
     * Set the list of pending that will be processed during execution
     * before the processor starts reading from the job channel.
     * </p> 
     */
    public void setPendingJobs(LinkedList<Job> pendingJobs)
    {
        this.pendingJobs = pendingJobs;
    }
    
    /**
     * <p>
     * Add some pending jobs to process before reading the JOB_CHANNEL. This is useful
     * when a processor was previously disconnected from the network and it (or a replacement
     * for it) is being connected and must pick up on all jobs that remained unprocessed
     * after the disconnection
     * </p>
     * 
     * @param newPendingJobs
     */
    public void addPendingJobs(List<Job> newPendingJobs)
    {
        if (pendingJobs == null)
            pendingJobs = new LinkedList<Job>();
        pendingJobs.addAll(newPendingJobs);
    }

    /**
     * <p>
     * Append a job to the list of pending jobs that will be 
     * processed during execution before the processor starts reading 
     * from the job channel.
     * </p> 
     */
    public void addPendingJob(Job job)
    {
       if (pendingJobs == null)
           pendingJobs = new LinkedList<Job>();
       pendingJobs.add(job);
    }
    
    /**
     * <p>Return true if monitoring is on and false otherwise.</p>
     */
    public boolean isMonitoringOn()
    {
        return monitoringOn;
    }

    /**
     * <p>
     * Turn monitoring on (if parameter is true) or off (if parameter is false).
     * </p>  
     */
    public void setMonitoringOn(boolean monitoringOn)
    {
        this.monitoringOn = monitoringOn;
    }
    
    public void addJobListener(JobListener<ContextType> listener)
    {
        listeners.add(listener);
    }
    
    public void removeJobListener(JobListener<ContextType> listener)
    {
        listeners.remove(listener);
    }
}
