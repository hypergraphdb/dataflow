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
 * Represents a remote peer that "runs and owns" a copy of the processing
 * network, as seen by a <code>DiskoJobMaster</code> peer. 
 * </p>
 * 
 * @author Borislav Iordanov
 *
 * @param <JobType>
 */
public class NetworkMasterPeer
{
    private JobFuture<Object> currentJob = null;

    public JobFuture<Object> getCurrentJob()
    {
        return currentJob;
    }

    public void setCurrentJob(JobFuture<Object> currentJob)
    {
        this.currentJob = currentJob;
    }    
}
