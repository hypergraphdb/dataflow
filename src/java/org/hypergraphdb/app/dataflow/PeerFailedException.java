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

import org.hypergraphdb.peer.HGPeerIdentity;

public class PeerFailedException extends RuntimeException
{
    private static final long serialVersionUID = -1;
    
    private HGPeerIdentity peer;
    
    public PeerFailedException(HGPeerIdentity peer)
    {
        this.peer = peer;
    }
    
    public PeerFailedException(HGPeerIdentity peer, Throwable cause)
    {
        super(cause);
        this.peer = peer;
    }
    
    public PeerFailedException(HGPeerIdentity peer, String msg)
    {
        super(msg);
        this.peer = peer;
    }
    
    public PeerFailedException(HGPeerIdentity peer, String msg, Throwable cause)
    {
        super(msg, cause);
        this.peer = peer;
    }
    
    public HGPeerIdentity getPeer()
    {
        return peer;
    }
}
