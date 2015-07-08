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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Mapping;
import org.hypergraphdb.util.Pair;

/**
 * <p>
 * Load balances a given dataflow processing node by maintaining a list of
 * copies of that node, splitting the original input evenly between them and
 * combining the output. The original node is translated into one 
 * {@link InputSplitter} that connects to all its input channels, one
 * {@link OutputCombiner} that connects to all its output channels and 
 * <code>N</code> intermediate clones. Cloning is done by a "deep" copy of the original
 * <code>Processor</code> instance - all bean properties, primitive arrays, standard
 * Java collections and maps are copied. 
 * </p>
 * 
 * <p>
 * A <code>LoadBalancer</code> instance itself is not part of the dataflow 
 * network. It is simply useful as a packaging of the whole load balancing
 * sub-network as well as providing the ability to add new load balanced
 * node (see the {@link expand} method) and remove some of them (see the
 * {@link shrink} method). Both <code>expand</code> and <code>shrink</code>
 * assume that the network is currently not running and can be safely 
 * modified.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 * @param <ContextType>
 */
public class LoadBalancer<ContextType>
{
    private static final String INPUT_CHANNEL_PREFIX = "LBIN";
    private static final String OUTPUT_CHANNEL_PREFIX = "LBOUT";
    
    private static AtomicInteger prefix_id = new AtomicInteger(0);
    
    private DataFlowNetwork<ContextType> network;
    private Integer channelsId;
    private Processor<ContextType> original;
    private Processor<ContextType> inputSplitter = null;
    private Processor<ContextType> outputCombiner = null;
    private List<Processor<ContextType>> copies = new ArrayList<Processor<ContextType>>();       
    
    public Processor<ContextType> getOriginal()
    {
        return original;
    }

    public void setOriginal(Processor<ContextType> original)
    {
        this.original = original;
    }

    public Processor<ContextType> getInputSplitter()
    {
        return inputSplitter;
    }

    public void setInputSplitter(Processor<ContextType> inputSplitter)
    {
        this.inputSplitter = inputSplitter;
    }

    public Processor<ContextType> getOutputCombiner()
    {
        return outputCombiner;
    }

    public void setOutputCombiner(Processor<ContextType> outputCombiner)
    {
        this.outputCombiner = outputCombiner;
    }

    public List<Processor<ContextType>> getCopies()
    {
        return copies;
    }

    public void setCopies(List<Processor<ContextType>> copies)
    {
        this.copies = copies;
    }

    @SuppressWarnings("unchecked")
    public void shrink(int toRemove)
    {        
        for (int i = 0; i < toRemove; i++)
        {
            int current = copies.size() - 1;
            Processor<ContextType> p = copies.get(current);
            HashSet<String> inputs = new HashSet<String>();
            inputs.addAll(network.getInputs(p));
            for (String in : inputs)
                if (!originalChannelId(in).equals(in))
                    network.removeChannel(in);
            HashSet<String> outputs = new HashSet<String>();
            outputs.addAll(network.getOutputs(p));
            for (String out : outputs)
                if (!originalChannelId(out).equals(out))
                    network.removeChannel(out);
            network.removeNode(p);
            copies.remove(current);
        }
        InputSplitter<ContextType> splitter = (InputSplitter<ContextType>) 
            ((inputSplitter instanceof JobProcessor) ? 
             ((JobProcessor)inputSplitter).getProcessor() : 
             inputSplitter);
        OutputCombiner<ContextType> combiner = (OutputCombiner<ContextType>) 
            ((outputCombiner instanceof JobProcessor) ? 
             ((JobProcessor)outputCombiner).getProcessor() : 
             outputCombiner); 
        splitter.setN(splitter.getN() - toRemove);
        combiner.setN(combiner.getN() - toRemove);
    }
    
    @SuppressWarnings("unchecked")
    public void expand(int toAdd)
    {
        int currentSize = copies.size();
        for (int i = currentSize; i < currentSize + toAdd; i++)
            try
            {
                copies.add(cloneProcessor(network, 
                                          original,
                                          network.getInputs(inputSplitter),
                                          network.getOutputs(outputCombiner),
                                          channelsId,
                                          i,
                                          network.getOutputs(inputSplitter), 
                                          network.getInputs(outputCombiner)));
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }         
        InputSplitter<ContextType> splitter = (InputSplitter<ContextType>) 
            ((inputSplitter instanceof JobProcessor) ? 
             ((JobProcessor)inputSplitter).getProcessor() : 
             inputSplitter);
        OutputCombiner<ContextType> combiner = (OutputCombiner<ContextType>) 
            ((outputCombiner instanceof JobProcessor) ? 
             ((JobProcessor)outputCombiner).getProcessor() : 
             outputCombiner); 
        splitter.setN(splitter.getN() + toAdd);
        combiner.setN(combiner.getN() + toAdd);            
    }
    
    private static String makeInputChannelPrefix(Integer id)
    {
        return INPUT_CHANNEL_PREFIX + "_" + id + "@";
    }
    
    private static String makeOutputChannelPrefix(Integer id)
    {
        return OUTPUT_CHANNEL_PREFIX + "_" + id + "@";
    }
    
    @SuppressWarnings("unchecked")
    private static <ContextType> Processor<ContextType> 
        cloneProcessor(DataFlowNetwork<ContextType> network, 
                       Processor<ContextType> processor, 
                       Set<String> inputs,
                       Set<String> outputs,
                       Integer channelsId,
                       Integer processorId,
                       Set<String> splitterOutputs, 
                       Set<String> combinerInputs) throws Exception
    {
        Mapping<Pair<Object, String>, Boolean> propertyFilter = 
            new Mapping<Pair<Object, String>, Boolean>() {
                public Boolean eval(Pair<Object, String> p)
                {
                    return !(p.getFirst() instanceof Processor) ||
                           !p.getSecond().equals("network");
                }
            
        };
        
        boolean isJobProcessor = processor instanceof JobProcessor;
        
        Processor<ContextType> copy = (Processor<ContextType>)HGUtils.cloneObject(processor,
                                                                             propertyFilter);
        Set<String> copyInputs = new HashSet<String>();
        for (String inId : inputs)
        {
            if (isJobProcessor && !JobDataFlow.isChannelJobSpecific(inId))
            {
                copyInputs.add(inId);
                combinerInputs.add(inId);
                continue;
            }
            String copyId = makeChannelId(inId, makeInputChannelPrefix(channelsId), processorId); 
            copyInputs.add(copyId);                
            Channel<?> ch = network.getChannel(inId);
            network.addChannel(new Channel(copyId,ch.getEOS(), ch.getCapacity()));
            splitterOutputs.add(copyId);
        }
        Set<String> copyOutputs = new HashSet<String>();
        for (String outId : outputs)
        {
            if (isJobProcessor && !JobDataFlow.isChannelJobSpecific(outId))
            {
                copyOutputs.add(outId);      
                splitterOutputs.add(outId);
                continue;
            }
            String copyId = makeChannelId(outId, makeOutputChannelPrefix(channelsId), processorId);
            copyOutputs.add(copyId);
            Channel<?> ch = network.getChannel(outId);
            network.addChannel(new Channel(copyId,ch.getEOS(), ch.getCapacity()));
            combinerInputs.add(copyId);                
        }
        
        if (isJobProcessor)
        {
            JobProcessor jp = (JobProcessor)copy;
            jp.setProcessor(new LoadBalancedNode(jp.getProcessor()));
        }
        else
            copy = new LoadBalancedNode(copy);
        network.addNode(copy, copyInputs, copyOutputs);    
        return copy;
    }
    
    @SuppressWarnings("unchecked")
    public static <ContextType> LoadBalancer<ContextType> 
        make(DataFlowNetwork<ContextType> network,
             Processor<ContextType> processor, 
             int numberOfCopies) throws Exception
    {
        LoadBalancer result = new LoadBalancer();
        result.network = network;
        result.original = processor;
        
        if (numberOfCopies < 1)
            return result;
        
        boolean isJobProcessor = processor instanceof JobProcessor;
        if (isJobProcessor && !(network instanceof JobDataFlow)) 
            throw new IllegalArgumentException("A non JobDataFlow network containing a JobProcessor.");

        result.channelsId = prefix_id.incrementAndGet();
        
        Set<String> inputs = new HashSet<String>();
        inputs.addAll(network.getInputs(processor));
        
        Set<String> outputs = new HashSet<String>();
        outputs.addAll(network.getOutputs(processor));
        
        Set<String> splitterOutputs = new HashSet<String>();
        Set<String> combinerInputs = new HashSet<String>();
        
        network.removeNode(processor);
                        
        for (int i = 0; i < numberOfCopies; i++)
            result.copies.add(cloneProcessor(network, 
                                             processor,
                                             inputs,
                                             outputs,
                                             result.channelsId,
                                             i,
                                             splitterOutputs, 
                                             combinerInputs));
        
        result.inputSplitter = new InputSplitter(numberOfCopies, 
                                                 makeInputChannelPrefix(result.channelsId));
        result.outputCombiner = new OutputCombiner(numberOfCopies, 
                                                   makeOutputChannelPrefix(result.channelsId));
        
        if (isJobProcessor)
        {
            result.inputSplitter = new JobProcessor((JobDataFlow)network, result.inputSplitter);
            result.outputCombiner = new JobProcessor((JobDataFlow)network, result.outputCombiner);
        }
        
        network.addNode(result.inputSplitter, 
                        inputs, 
                        splitterOutputs);
        network.addNode(result.outputCombiner, 
                        combinerInputs, 
                        outputs);        
        return result;
    }
    
    public static String makeChannelId(String originalId, String prefix, int index)
    {
        return prefix + originalId + "@" + Integer.toString(index);
    }
    
    public static String originalChannelId(String id)
    {
        int from = id.indexOf('@');
        int to = id.lastIndexOf('@');
        if (from < 0 || to < 0)
            return id;
        else
            return id.substring(from + 1, to);
    }
    
}
