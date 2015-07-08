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

import java.util.Set;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.atom.HGBergeLink;

/**
 * 
 * <p>
 * Used to persist a Disko <code>Channel</code> in a HyperGraphDB instance as
 * a <code>HGBergeLink</code> (i.e. a link where a subset of the targets is 
 * designated as the <b>head</b> while its complement is the <b>tail</b>.) 
 * </p>
 *
 * @author Borislav Iordanov
 *
 * @param <T>
 */
public class ChannelLink<T> extends HGBergeLink
{
    private Channel<T> channel;
    
    public ChannelLink(HGHandle...targets)
    {
        super(targets);
    }

    public ChannelLink(Channel<T> channel, int tailIndex, HGHandle...targets)
    {
        super(targets);
        this.channel = channel;
        setTailIndex(tailIndex);
    }
    
    public ChannelLink(Channel<T> channel, Set<HGHandle> head, Set<HGHandle> tail)
    {
        HGHandle [] targets = new HGHandle[head.size() + tail.size()];
        System.arraycopy(head.toArray(), 0, targets, 0, head.size());
        System.arraycopy(tail.toArray(), 0, targets, head.size(), tail.size());
        this.outgoingSet = targets;
        this.setTailIndex(head.size());
        this.channel = channel;
    }
    
    public Channel<T> getChannel()
    {
        return channel;
    }

    public void setChannel(Channel<T> channel)
    {
        this.channel = channel;
    }
    
    public String toString()
    {
        return channel.toString();
    }
}
