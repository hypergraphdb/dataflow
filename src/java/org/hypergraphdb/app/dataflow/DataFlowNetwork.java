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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.hypergraphdb.util.Pair;

/**
 * <p>
 * Represents and manages the computation of a complete dataflow network. A dataflow network
 * can be thought of as a complete, parallel program to be executed on a set of concurrent threads.
 * An instance of this class manages all {@link Channel}s and {@link Processor}s comprising the 
 * network.    
 * </p>
 * 
 * <p>
 * Each network has a global, arbitrary context object for all processing nodes to access
 * whatever global information the application needs to manage. The context has to be set
 * before the network is started (e.g. at construction time) and it is passed to each 
 * process at execution time.
 * </p>
 * 
 * <p>
 * To create a network, first create an instance of this class. Then add all communication 
 * {@link Channel}s, finally add all {@link Processor}s. Note that all channels must have 
 * unique names with the network. 
 * </p>
 * 
 * <p>
 * To execute it, just call the {@link DataFlowNetwork.start} method. The start method returns
 * a <code>Future</code> object that can be polled or waited upon until the computation terminates.
 * Once the computation terminates, gracefully or due to a cancellation, you can start the network
 * again.
 * </p>
 * 
 * <p>
 * The network maintains a thread pool internally so by default, the Java application won't exit
 * normally until the network is <code>shutdown</code> (which shuts down the pool). You can force
 * the threads to be created as daemons with the <code>daemon</code>. 
 * </p>
 * 
 * @author Borislav Iordanov
 *
 * @param <ContextType> The type of the global, network context. It must be the same type
 * as all {@link Processor}s participating in this network. 
 */
public class DataFlowNetwork<ContextType>
{
    boolean daemon;
    Map<Object, Channel<?>> channels = new HashMap<Object, Channel<?>>();;
    Map<Processor<ContextType>, Pair<Set<String>, Set<String>>>
        nodes = new HashMap<Processor<ContextType>, Pair<Set<String>, Set<String>>>();
    Map<String, Set<Processor<ContextType>>> channelReaders = 
        new HashMap<String, Set<Processor<ContextType>>>();
    Map<String, Set<Processor<ContextType>>> channelWriters = 
        new HashMap<String, Set<Processor<ContextType>>>();

//    Set<ProcessingNode<ContextType>> nodes;
        
    transient ContextType context;

    final transient ReentrantLock mainLock = new ReentrantLock();
    final transient Condition termination = mainLock.newCondition(); // signal

    // termination when runningNodes goes to 0
    final transient Condition initiation = mainLock.newCondition();
    transient int runningNodes = 0; // updated by ProcessingNodes
    transient ThreadPoolExecutor threadPool = null;
    
    private void initThreadPool()
    {
        if (threadPool != null && !threadPool.isTerminated())
            return;

        // We want to make sure that the threads in the thread pool are using
        // the current thread's class loader.
        final ClassLoader loader = Thread.currentThread().getContextClassLoader();
        threadPool = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L,
                                            TimeUnit.SECONDS,
                                            new SynchronousQueue<Runnable>(),
                                            new ThreadFactory()
                                            {
                                                public Thread newThread(Runnable r)
                                                {
                                                    Thread t = new Thread(r);
                                                    t.setContextClassLoader(loader);
                                                    t.setDaemon(daemon);
                                                    return t;
                                                }
                                            });
    }

    protected void startNode(Processor<ContextType> processor, Ports ports)
    {
        ProcessingNode<ContextType> node = new ProcessingNode<ContextType>(this,
                processor,
                ports);
        threadPool.submit(node);        
    }
    
    protected void startNode(Processor<ContextType> processor)
    {
        Pair<Set<String>, Set<String>> chIds = nodes.get(processor);
        if (chIds == null)
            throw new RuntimeException("Processor " + processor + " not part of this network.");
        Set<String> inputs = chIds.getFirst();
        Set<String> outputs = chIds.getSecond();
        Ports ports = new Ports();
        for (String inId : inputs)
        {
            Channel<?> channel = channels.get(inId);
            if (channel == null)
                throw new IllegalArgumentException("Input Channel "
                                                   + inId
                                                   + " wasn't added to this network.");
            ports.addPort(channel.newInputPort());
        }

        for (String outId : outputs)
        {
            Channel<?> channel = channels.get(outId);
            if (channel == null)
                throw new IllegalArgumentException("Output Channel "
                                                   + outId
                                                   + " wasn't added to this network.");
            ports.addPort(channel.newOutputPort());
        }
        startNode(processor, ports);
    }
    
    /**
     * <p>
     * Construct a network with a <code>null</code> global context.
     * </p>
     */
    public DataFlowNetwork()
    { 
    }

    /**
     * <p>
     * Construct a network with the specified global context that will be passed as the 
     * first argument to all its {@link Processor}s.
     * </p>
     * @param ctx The global context.
     */
    public DataFlowNetwork(ContextType ctx)
    {
        this();
        this.context = ctx;
    }

    /**
     * <p>
     * Construct a network with the specified global context that will be passed as the 
     * first argument to all its {@link Processor}s, and be explicitly setting whether
     * threads should be created as daemons.
     * </p>
     * @param ctx The global context.
     * @param daemon If <code>true</code>, threads in the thread pool will be created as daemons, so
     * the JVM won't wait for them to finish before exiting.
     */
    public DataFlowNetwork(ContextType ctx, boolean deamon)
    {
        this(ctx);
        this.daemon = deamon;
    }
    
    public boolean isDaemon()
    {
        return daemon;
    }

    public void setDaemon(boolean deamon)
    {
        this.daemon = deamon;
    }

    /**
     * <p>Clear all internal data structures - processing nodes, channels etc. This
     * method assumes that the network is currently <strong>not</strong> running.</p>
     */
    public void clearAll()
    {
        nodes.clear();
        channels.clear();
        channelReaders.clear();
        channelWriters.clear();
    }
    
    public void addChannel(Channel<?> channel)
    {
        channels.put(channel.getId(), channel);
    }

    public Collection<Channel<?>> getChannels()
    {
        return channels.values();
    }

    @SuppressWarnings("unchecked")
    public <D> Channel<D> getChannel(String id)
    {
        return (Channel<D>)channels.get(id);
    }
    
    public void removeChannel(String id)
    {
        Channel<?> ch = channels.get(id);
        for (Processor<ContextType> reader : getReaders(ch))
            getInputs(reader).remove(id);
        for (Processor<ContextType> writer : getWriters(ch))
            getOutputs(writer).remove(id);
        channels.remove(id);
        channelReaders.remove(id);
        channelWriters.remove(id);
    }
    
    public Set<String> getInputs(Processor<ContextType> processor)
    {
        Pair<Set<String>, Set<String>> chids = nodes.get(processor);
        return chids.getFirst();
    }

    public Set<Processor<ContextType>> getReaders(Channel<?> ch)
    {
        Set<Processor<ContextType>> result = channelReaders.get(ch.getId());
        return result != null ? result : new HashSet<Processor<ContextType>>();
    }

    public Set<Processor<ContextType>> getWriters(Channel<?> ch)
    {
        Set<Processor<ContextType>> result = channelWriters.get(ch.getId());
        return result != null ? result : new HashSet<Processor<ContextType>>();
    }
    
    public Set<String> getOutputs(Processor<ContextType> processor)
    {
        Pair<Set<String>, Set<String>> chids = nodes.get(processor);
        return chids.getSecond();
    }
    
    public Set<Processor<ContextType>> getNodes()
    {
        return nodes.keySet();
    }
    
    public void removeNode(Processor<ContextType> processor)
    {        
        Pair<Set<String>, Set<String>> p = nodes.remove(processor);
        for (String chId : p.getFirst())
        {
            Set<Processor<ContextType>> S = channelReaders.get(chId);
            if (S != null)
                S.remove(processor);
        }
        for (String chId : p.getSecond())
        {
            Set<Processor<ContextType>> S = channelWriters.get(chId);
            if (S != null)
                S.remove(processor);
        }        
    }

    public void addNode(Processor<ContextType> processor, 
                        String [] inputs,
                        String [] outputs)
    {
        Set<String> ins = new HashSet<String>();
        for (String i : inputs) ins.add(i);
        Set<String> outs = new HashSet<String>();
        for (String i : outputs) outs.add(i);
        addNode(processor, ins, outs);
    }
    
    public void addNode(Processor<ContextType> processor, 
                        Set<String> inputs,
                        Set<String> outputs)
    {
        nodes.put(processor, new Pair<Set<String>, Set<String>>(inputs, outputs));
        for (String chId : inputs)
        {
            Set<Processor<ContextType>> S = channelReaders.get(chId);
            if (S == null)
            {
                S = new HashSet<Processor<ContextType>>();
                channelReaders.put(chId, S);
            }
            S.add(processor);
        }
        for (String chId : outputs)
        {
            Set<Processor<ContextType>> S = channelWriters.get(chId);
            if (S == null)
            {
                S = new HashSet<Processor<ContextType>>();
                channelWriters.put(chId, S);
            }
            S.add(processor);
        }        
    }

    /**
     * <p>
     * Merge the passed in network into this one. Duplicate channels are
     * detected and merged, but not so for processors which do not carry unique
     * identifiers.
     * </p>
     * 
     * @param other
     *            The <code>DataFlowNetwork</code> to merge into this one.
     */
    public void mergeWith(DataFlowNetwork<ContextType> other)
    {
        throw new UnsupportedOperationException("this is a TODO...");
    }

    private Future<Boolean> execute()
    {
        if (nodes.isEmpty())
            return new PresentFuture<Boolean>();
        initThreadPool();
        for (Processor<ContextType> node : nodes.keySet())
        {
            try
            {
                startNode(node);
            }
            catch (RejectedExecutionException ex)
            {
                // thrown when another thread calls 'shutdown' while we are
                // try to initiate a new network execution
                return new PresentFuture<Boolean>();
            }
        }
        initiated();
        return new Future<Boolean>()
        {
            private boolean cancelled = false;

            public boolean cancel(boolean mayInterruptIfRunning)
            {
                if (mayInterruptIfRunning)
                    threadPool.shutdownNow();
                else
                    threadPool.shutdown();
                completed();
                return cancelled = true;
            }

            public Boolean get() throws InterruptedException,
                                ExecutionException
            {
                mainLock.lock();
                try
                {
                    if (isDone())
                        return !isCancelled();
                    termination.await();
                }
                finally
                {
                    mainLock.unlock();
                }
                return !isCancelled();
            }

            public Boolean get(long timeout, TimeUnit unit)
                                                           throws InterruptedException,
                                                           ExecutionException,
                                                           TimeoutException
            {
                mainLock.lock();
                try
                {
                    if (isDone())
                        return !isCancelled();
                    termination.await(timeout, unit);
                }
                finally
                {
                    mainLock.unlock();
                }
                return !isCancelled();
            }

            public boolean isCancelled()
            {
                return cancelled;
            }

            public boolean isDone()
            {
                return isIdle();
            }
        };
    }

    public synchronized Future<Boolean> start()
    {
        if (!isIdle())
            throw new DataFlowException("DataFlowNetwork already running "
                                        + this);
        return execute();
    }

    public synchronized Future<Boolean> start(ContextType context)
    {
        if (!isIdle())
            throw new DataFlowException("DataFlowNetwork already running "
                                        + this);
        setContext(context);
        return execute();
    }

    public synchronized boolean isIdle()
    {
        return !activeNetworks.contains(this);
    }

    public void shutdown()
    {
        kill();
    }

    /**
     * 
     * <p>
     * Interrupt all currently running nodes of network and force them to exit.
     * This will behave well if and only if all processing nodes respond well to
     * a thread interrupt.
     * </p>
     * 
     */
    public void kill()
    {    	
    	if (threadPool != null)
    	{
    		threadPool.shutdownNow();
    		completed();
    	}
    }

    protected void finalize()
    {
        kill();
    }

    /**
     * 
     * <p>
     * Called by <code>execute</code> to signal that all processing nodes have
     * been submitted to the thread pool.
     * </p>
     * 
     */
    protected synchronized void initiated()
    {
        mainLock.lock();
        activeNetworks.add(this);
        mainLock.unlock();
    }

    /**
     * 
     * <p>
     * Called (usually by the last <code>ProcessingNode</code>) to signal
     * that the network has completed.
     * </p>
     * 
     */
    protected synchronized void completed()
    {
        activeNetworks.remove(this);
    }

    public ContextType getContext()
    {
        return context;
    }

    public synchronized void setContext(ContextType context)
    {
        if (!isIdle())
            throw new DataFlowException(
              "The execution context of a network can be changed only during iddle time.");
        this.context = context;
    }

    // Shutting down all active DataFlow networks. This is important when the
    // application must exit
    // graciously by responding to an external signal such stopping an OS
    // service. We maintain a
    // list of all active networks and trigger a shutdownNow operation on them.

    private static Set<DataFlowNetwork<?>> activeNetworks = 
    	Collections.synchronizedSet(new HashSet<DataFlowNetwork<?>>());

    private static class DataFlowOnShutDown implements Runnable
    {
        public void run()
        {
            for (DataFlowNetwork<?> nw : activeNetworks)
            {
                try
                {
                    nw.kill();
                }
                catch (Throwable t)
                {
                    System.err.println("Exception while shutting down " + nw
                                       + ", stack trace follows...");
                    t.printStackTrace(System.err);
                }
            }
        }
    }

    static
    {
        Runtime.getRuntime()
               .addShutdownHook(new Thread(new DataFlowOnShutDown()));
    }

    // A dummy Future object for an already completed task that returns null.
    private static class PresentFuture<V> implements Future<V>
    {
        public boolean cancel(boolean mayInterruptIfRunning)
        {
            return false;
        }

        public V get() throws InterruptedException, ExecutionException
        {
            return null;
        }

        public V get(long timeout, TimeUnit unit) throws InterruptedException,
                                                 ExecutionException,
                                                 TimeoutException
        {
            return null;
        }

        public boolean isCancelled()
        {
            return false;
        }

        public boolean isDone()
        {
            return true;
        }
    }
}
