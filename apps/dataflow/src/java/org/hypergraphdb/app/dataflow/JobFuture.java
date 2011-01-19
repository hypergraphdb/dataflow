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

import org.hypergraphdb.util.CallbackFuture;

public class JobFuture<T> extends CallbackFuture<T>
{
	private Job theJob;
 	private long submittedAt;
 	private long scheduledAt;
 	private long completedAt;
 	
	public JobFuture(Job theJob)
	{
		this.theJob = theJob;
	}	
	
	public Job getJob()
	{
		return theJob;
	}

	public long getSubmittedAt()
	{
		return submittedAt;
	}

	public void setSubmittedAt(long submittedAt)
	{
		this.submittedAt = submittedAt;
	}

	public long getScheduledAt()
	{
		return scheduledAt;
	}

	public void setScheduledAt(long scheduledAt)
	{
		this.scheduledAt = scheduledAt;
	}

	public long getCompletedAt()
	{
		return completedAt;
	}

	public void setCompletedAt(long completedAt)
	{
		this.completedAt = completedAt;
	}	
}
