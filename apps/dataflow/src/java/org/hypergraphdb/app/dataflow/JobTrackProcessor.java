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

import java.util.LinkedList;
import java.util.List;

import org.hypergraphdb.HGGraphHolder;
import org.hypergraphdb.HyperGraph;

/**
 * <p>
 * The purpose of this processor is to listen on the Job channel and save
 * each incoming  job in the HGDB database of the analysis context. There 
 * is one such processor running at each peer.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 * @param <DocumentType>
 * @param <JobType>
 */
public class JobTrackProcessor<Context, JobType extends Job> 
     implements Processor<Context>, HGGraphHolder
{
    volatile transient Object disconnectSignal = null;
    // Store unprocessed jobs from the JOB_CHANNEL InputPort buffer when
    // this processor is explicitly disconnected.
    LinkedList<JobType> pendingJobs = null;
    private HyperGraph graph;
    
    public String getName()
    {
        return "Job Tracking";
    }

    public void process(Context ctx, Ports ports) throws InterruptedException
    {
        InputPort<JobType> jobIn = ports.getInput(JobDataFlow.JOB_CHANNEL_ID);
        while (disconnectSignal == null)
        {
            JobType job = null;
            if (pendingJobs != null && !pendingJobs.isEmpty())
                job = pendingJobs.removeFirst();
            else
            {
                // We don't use jobIn.take here so that we can periodically check
                // whether we're being disconnected.
                job = jobIn.poll();
                if (job == null)                
                    try { Thread.sleep(5000); continue; }
                    catch (InterruptedException ex) { break; }
            }
            if (jobIn.isEOS(job))
                break;
            
            if (graph.get(job.getHandle()) == null)
                graph.define(graph.getPersistentHandle(job.getHandle()),
                			 job);
            if (disconnectSignal != null)
            {
                pendingJobs = new LinkedList<JobType>();
                for (JobType pending = jobIn.poll(); 
                     pending != null && 
                     !jobIn.isEOS(pending); pending = jobIn.poll())
                    pendingJobs.add(pending);
                synchronized (disconnectSignal)
                {
                    disconnectSignal.notifyAll();
                }
            }            
        }        
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
     * Return the list of un-processed jobs that were submitted to this particular
     * processor, but were never started because the processor was explicitly 
     * disconnected by a call to <code>disconnect</code>.
     * </p>
     */
    public List<JobType> getPendingJobs()
    {
        return pendingJobs;
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
    public void addPendingJobs(List<JobType> newPendingJobs)
    {
        if (pendingJobs == null)
            pendingJobs = new LinkedList<JobType>();
        pendingJobs.addAll(newPendingJobs);
    }

    public void setHyperGraph(HyperGraph graph)
	{
		this.graph = graph;
	} 	
}
