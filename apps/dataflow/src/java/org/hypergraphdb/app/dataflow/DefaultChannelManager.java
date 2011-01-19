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

import java.util.Collections;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unchecked")
public class DefaultChannelManager implements ChannelManager
{
    private transient Map<Job, Map<String, JobChannel<?>>> jobChannels = 
        Collections.synchronizedMap(new HashMap<Job, Map<String, JobChannel<?>>>()); 

    protected <D> JobChannel<D> createNewChannel(JobDataFlow<?> network, 
                                                          Channel<D> logicalChannel, 
                                                          Job job)
    {
        return new JobChannel<D>(
                network.getReaders(logicalChannel).size(),
                logicalChannel.getId(), 
                job, 
                logicalChannel.getEOS(),
                logicalChannel.getCapacity());        
    }
    
    public <D> JobChannel<D> getJobChannel(JobDataFlow<?> network, 
                                           Channel<D> logicalChannel, 
                                           Job job)
    {
        synchronized(jobChannels)
        {
            Map<String, JobChannel<?>> all = jobChannels.get(job);
            if (all == null)
            {
                all = new HashMap<String, JobChannel<?>>();
                jobChannels.put(job, all);
            }
            JobChannel<D> ch = (JobChannel<D>)all.get(logicalChannel.getId());
            if (ch == null)
            {                
                ch = createNewChannel(network, logicalChannel, job);
                all.put(logicalChannel.getId(), ch);
            }
            return ch;
        }
    }

    public void notifyJobDone(Job job)
    {
        synchronized (jobChannels)
        {
        	//DistUtils.java.log.info("Cleanup DefaultChannelManager from job " + job);
            jobChannels.remove(job);
        }
    }    
}
