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

public class DistributedException extends RuntimeException
{
    private static final long serialVersionUID = -1;
    
    private Object node;
    private String message;
    private Throwable cause;
    private Job job;
    
    public DistributedException()
    {        
    	this(null, null, null, null);
    }
    
    public DistributedException(Object node, String msg) 
    {
    	this(node, null, msg, null);
    }
    
    public DistributedException(Object node, String msg, Throwable cause) 
    {
    	this(node, null, msg, cause);
    }

    public DistributedException(Object node, Job job, String msg, Throwable cause)
    {
    	this.node = node;
    	this.job = job;
    	this.message = msg;
    	this.cause = cause;
    }
    
    public Object getNode()
    {
        return node;
    }

    public void setNode(Object node)
    {
        this.node = node;
    }

    public String getMessage()
    {
        return message;
    }

    public void setMessage(String message)
    {
        this.message = message;
    }

    public Throwable getCause()
    {
        return cause;
    }

    public void setCause(Throwable cause)
    {
        this.cause = cause;
    }

	public Job getJob()
	{
		return job;
	}

	public void setJob(Job job)
	{
		this.job = job;
	}   
}
