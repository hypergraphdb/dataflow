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

/**
 * <p>
 * A <code>ChannelManager</code> is responsible for managing different channel instances
 * on a per-job basis. When several jobs are being processed simultaneously on the  
 * data-flow network, we must ensure that the flow of data is isolated for each job. This
 * is achieved by a per job copy for each channel. The channel manager handles those copies. 
 * </p>
 * 
 * @author Borislav Iordanov
 *
 * @param <JobType>
 */
public interface ChannelManager
{
    <D> JobChannel<D> getJobChannel(JobDataFlow<?> network, 
                                    Channel<D> logicalChannel, 
                                    Job job);
    /**
     * <p>
     * Notify channel manager that a given job has finished executing and 
     * any data structures pertaining to it should be cleared. 
     * </p>
     * 
     * @param job The job that has completed.
     */
    void notifyJobDone(Job job);
}
