package sample.dataflow;

import java.util.concurrent.Future;

import org.hypergraphdb.app.dataflow.*;

/**
 * 
 * <p>
 * This is a very simple example demonstrating the basic data flow API. The main
 * program constructs a network that takes a single stream of integers input and
 * calculates the sums of all its even numbers, all its odd numbers and the
 * total sum. To do that, it splits the input into a stream of even numbers and
 * a stream of odd numbers. The sum of each of those streams is accumulated
 * separately and finally put back together.
 * </p>
 * 
 * <p>
 * The processing nodes here are implemented by a few very simple classes, for
 * illustration purposes only.
 * </p>
 * 
 * @author Borislav Iordanov
 * 
 */
public class DataFlowArithmetic
{
    /**
     * 
     * <p>
     * Expected two inputs from two input channels and it writes the output to a
     * single output channel. The names of the channels are specified in the
     * constructor.
     * </p>
     * 
     * @author Borislav Iordanov
     * 
     */
    public static class Sum extends AbstractProcessor<Object>
    {
        private String left, right, output;

        public Sum()
        {
        }

        public Sum(String left, String right, String output)
        {
            this.left = left;
            this.right = right;
            this.output = output;
        }

        public void process(Object ctx, Ports ports)
                throws InterruptedException
        {
            InputPort<Integer> inLeft = ports.getInput(left);
            InputPort<Integer> inRight = ports.getInput(right);
            OutputPort<Integer> sumOut = ports.getOutput(output);
            for (Integer x : inLeft)
            {
                Integer y = inRight.take();
                if (inLeft.isEOS(y))
                    throw new RuntimeException(
                            "Sum is only defined on pair of numbers.");
                sumOut.put(x + y);
            }
        }
    }

    /**
     * 
     * <p>
     * Expects a single input stream and it continuously calculates the sum of
     * the numbers coming in, output it to its output channel only when the
     * input reaches EOS.
     * </p>
     * 
     * @author Borislav Iordanov
     * 
     */
    public static class Accumulator extends AbstractProcessor<Object>
    {
        private String inputChannel;
        private String outputChannel;

        public Accumulator()
        {
        }

        public Accumulator(String inputChannel, String outputChannel)
        {
            this.inputChannel = inputChannel;
            this.outputChannel = outputChannel;
        }

        public void process(Object ctx, Ports ports)
                throws InterruptedException
        {
            InputPort<Integer> in = ports.getInput(inputChannel);
            OutputPort<Integer> out = ports.getOutput(outputChannel);
            int total = 0;
            for (Integer x : in)
                total += x;
            out.put(total);
        }
    }

    /**
     * 
     * <p>
     * Split the input stream into even and odd, writing each to a different
     * channel. Here, the channel names are hard-coded in the implementation
     * rather than passed at construction time.
     * </p>
     * 
     * @author Borislav Iordanov
     * 
     */
    public static class SplitParity extends AbstractProcessor<Object>
    {
        public void process(Object ctx, Ports ports)
                throws InterruptedException
        {
            InputPort<Integer> in = ports.getInput("random-stream");
            OutputPort<Integer> outEven = ports.getOutput("even-numbers");
            OutputPort<Integer> outOdd = ports.getOutput("odd-numbers");
            for (Integer x : in)
            {
                if (x % 2 == 0)
                    outEven.put(x);
                else
                    outOdd.put(x);
            }
        }
    }

    /**
     * 
     * <p>
     * Prints to stdout whatever comes in from its single input port, prefixing
     * it with a predefined, at construction time, string.
     * </p>
     * 
     * @author Borislav Iordanov
     * 
     */
    public static class Printer extends AbstractProcessor<Object>
    {
        private String fromChannel;
        private String prefix;

        public Printer()
        {
        }

        public Printer(String prefix, String fromChannel)
        {
            this.fromChannel = fromChannel;
            this.prefix = prefix;
        }

        public void process(Object ctx, Ports ports)
                throws InterruptedException
        {
            InputPort<Integer> in = ports.getInput(fromChannel);
            for (Integer x = in.take(); !in.isEOS(x); x = in.take())
                System.out.println(prefix + x);
        }
    }

    public static class ConsoleLoggingProcessor extends AbstractProcessor<Object>
    {
        @Override
        public void process(Object ctx, Ports ports)
                throws InterruptedException
        {
            InputPort<Integer> in1 = ports.getInput("user-defined-channel");
            System.out.println("Starting processor ");
            for (Integer x = in1.take(); !in1.isEOS(x); x = in1.take())
            {
                System.out.println(x + " received");
            }
            System.out.println("Exiting processor");
        }
    }

    public static void main(String argv[])
    {
        while (true)
        {
            System.out.println("Starting network again");
            Integer EOF = Integer.MIN_VALUE;
            DataFlowNetwork<Object> network = new DataFlowNetwork<Object>();
            network.addChannel(new Channel<Integer>("user-defined-channel", EOF));

            ConsoleLoggingProcessor processor = new ConsoleLoggingProcessor();
            network.addNode(processor, new String[] { "user-defined-channel" },
                    new String[] {});

            try
            {
                Future<Boolean> f = network.start();
                Channel<Integer> ch = network.getChannel("user-defined-channel");
                Thread.currentThread().sleep(100);
                for (int i = 0; i < 10; i++)
                {
                    ch.put(i);
                    //Thread.currentThread().sleep(1);
                }

                ch.put(EOF);
                System.out.println("About to end");
                System.out.println("Network completed successfully: " + f.get());
                System.out.println("gotten");
            }
            catch (Throwable t)
            {
                t.printStackTrace(System.err);
            }
            finally
            {
                network.shutdown();
            }
        }
    }

    public static void main2(String argv[])
    {
        // All channels work with the same data type (Integer) for all inputs
        // and
        // outputs. And we postulate the value Integer.MIN_VALUE will serve as
        // an
        // End-Of-Stream marker.
        Integer EOF = Integer.MIN_VALUE;

        // To construct a network, create a DataFlowNetwork instance,
        // parameterized
        // by the type of global context that all processing nodes share. In
        // this
        // case we are not using any global context, so we leave the type as
        // Object.
        DataFlowNetwork<Object> network = new DataFlowNetwork<Object>();

        // First we add all the data channels to the network. Each channel is
        // uniquely identified by a name and it needs a special end-of-stream
        // marker.

        // The channel that serves as the main input to the whole network.
        // There may be multiple such channels that get data from the
        // "outside world".
        // Here we have only one.
        network.addChannel(new Channel<Integer>("random-stream", EOF));

        // The channel where the even number of the input stream go.
        network.addChannel(new Channel<Integer>("even-numbers", EOF));

        // The odd numbers channel.
        network.addChannel(new Channel<Integer>("odd-numbers", EOF));

        // etc..
        network.addChannel(new Channel<Integer>("even-sum", EOF));
        network.addChannel(new Channel<Integer>("odd-sum", EOF));
        network.addChannel(new Channel<Integer>("total-sum", EOF));

        // Next, we add the processing nodes using network.addNode.
        // The first argument of that method is
        // an arbitrary object implementing the Processor interface.
        // The second argument specifies all input channels that the processor
        // reads from and the third argument specifies all the output channels
        // that the processor writes to. The network will create a port
        // for each of those channels and pass it in the Ports argument
        // the Processor.process implementation.

        network.addNode(new SplitParity(), new String[] { "random-stream" },
                new String[] { "even-numbers", "odd-numbers" });
        network.addNode(new Accumulator("even-numbers", "even-sum"),
                new String[] { "even-numbers" }, new String[] { "even-sum" });
        network.addNode(new Accumulator("odd-numbers", "odd-sum"),
                new String[] { "odd-numbers" }, new String[] { "odd-sum" });
        network.addNode(new Sum("even-sum", "odd-sum", "total-sum"),
                new String[] { "even-sum", "odd-sum" },
                new String[] { "total-sum" });

        // We now attach a printer to each of the "sum" channel to output the
        // results.
        // Note that the results will be printed out in some random order
        // depending on thread
        // scheduling.
        network.addNode(new Printer("Even sum=", "even-sum"),
                new String[] { "even-sum" }, new String[0]);
        network.addNode(new Printer("Odd sum=", "odd-sum"),
                new String[] { "odd-sum" }, new String[0]);
        network.addNode(new Printer("Total sum=", "total-sum"),
                new String[] { "total-sum" }, new String[0]);

        try
        {
            // When we start the network, it creates a thread pool for the
            // processor to use and it opens all channels for read and write.
            // The network will remain in "working mode" until EOS is written
            // to all its ports. The returned Future from the start method can
            // be used to cancel processing, wait for it etc. The result is
            // true/false
            // depending on whether it completed gracefully or not.
            Future<Boolean> f = network.start();

            // So we just write some random integers to the "entry" channel
            // directly.
            Channel<Integer> ch = network.getChannel("random-stream");
            for (int i = 0; i < 1000; i++)
            {
                ch.put((int) (Math.random() * 10000));
            }

            // And we close it, triggering a cascading close operation of all
            // ports downstream in the network.
            ch.put(EOF);
            System.out.println("Network completed successfully: " + f.get());

            // At this point, the network is inactive, but it can be restarted
            // again.
            f = network.start();
            ch = network.getChannel("random-stream");
            for (int i = 0; i < 1000; i++)
            {
                ch.put((int) (Math.random() * 10000));
            }
            ch.put(EOF);
            System.out.println("Network completed successfully: " + f.get());
        }
        catch (Throwable t)
        {
            t.printStackTrace(System.err);
        }
        finally
        {
            // shutdown the thread pool of the network so the application can
            // exit gracefully.
            network.shutdown();
        }
    }
}