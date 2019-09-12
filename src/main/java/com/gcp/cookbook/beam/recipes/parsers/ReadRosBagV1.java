package com.gcp.cookbook.beam.recipes.parsers;

import com.github.swrirobotics.bags.reader.BagFile;
import com.github.swrirobotics.bags.reader.BagReader;
import com.github.swrirobotics.bags.reader.MessageHandler;
import com.github.swrirobotics.bags.reader.TopicInfo;
import com.github.swrirobotics.bags.reader.exceptions.BagReaderException;
import com.github.swrirobotics.bags.reader.exceptions.UninitializedFieldException;
import com.github.swrirobotics.bags.reader.messages.serialization.*;
import com.github.swrirobotics.bags.reader.records.Connection;
import com.github.swrirobotics.bags.reader.records.MessageData;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;

import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class ReadRosBagV1 {
    private static final Logger LOG = LoggerFactory.getLogger(ReadRosBagV1.class);


    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        runReadRosBagPipeline(options);
    }

    private static void runReadRosBagPipeline(Options options) {
        Pipeline pipeline = Pipeline.create(options);
        pipeline
                .apply("Find files", FileIO.match().filepattern(options.getInput()))
                .apply("Read matched files", FileIO.readMatches())
                //.apply("Read parquet files", ParquetIO.readFiles(SCHEMA))

                .apply("Get All Rosbag Topics ", ParDo.of(new DoFn<FileIO.ReadableFile, KV<String, KV<String,String>>>() {
                    @ProcessElement
                    public void process(ProcessContext c){
                        try {
                            FileIO.ReadableFile f = c.element();
                            BagFile bag = null;
                            String file = f.getMetadata().resourceId().toString();
                            final String[][] values = {new String[1]};
                                bag = BagReader.readFile(f.getMetadata().resourceId().toString());
                                bag.read();
                               // c.output(KV.of("file",file));
                                for (TopicInfo topic : bag.getTopics()) {
                                    System.out.println(topic.getName() + " \t\t" + topic.getMessageCount() +
                                            " msgs \t: " + topic.getMessageType() + " \t" +
                                            (topic.getConnectionCount() > 1 ? ("(" + topic.getConnectionCount() + " connections)") : ""));
                                    c.output(KV.of(file,KV.of("topic-"+topic.getName(), topic.getMessageType())));
                                    //c.output(KV.of("topic-"+topic.getName(), topic.getName()));

                                }

                            } catch (BagReaderException e) {
                                e.printStackTrace();
                            }
                    }
                }))

                .apply("Process CameraInfo", ParDo.of(new DoFn<KV<String, KV<String,String>>, KV<String, String>>() {
                    @ProcessElement
                    public void process(ProcessContext c){
                        try {
                            BagFile bag = null;
                            final ListIterator<String>[] litr = new ListIterator[]{null};

                               //final String[][] values = {new String[1]};
                               bag = BagReader.readFile(c.element().getKey());

                               KV<String,String> topics = c.element().getValue();

                              // System.out.println("C Element Values " + c.element().getValue());

                                   System.out.println("Topic Element Values " + c.element().getValue().getValue());


                                   bag.forMessagesOfType(topics.getValue(), new MessageHandler() {
                                       @Override
                                       public boolean process(MessageType message, Connection connection) {

                                           litr[0] = message.getFieldNames().listIterator();

                                           if(topics.getValue().contains("sensor_msgs/CameraInfo")) {
                                               MessageType roi = message.getField("roi");
                                               MessageType header = message.getField("header");
                                               UInt32Type seq = header.getField("seq");
                                               StringType frame_id = header.getField("frame_id");
                                               TimeType time = header.getField("stamp");
                                              // System.out.println(time.getValue().getTime());
                                               BoolType do_rectify = roi.getField("do_rectify");
                                               UInt32Type width = roi.getField("width");
                                               UInt32Type height = roi.getField("height");
                                               UInt32Type y_offset = roi.getField("y_offset");
                                               UInt32Type x_offset = roi.getField("x_offset");


                                               try {
                                                   c.output(KV.of("CameraInfo/Roi/do_rectify", Boolean.toString(do_rectify.getValue().booleanValue())));
                                                   c.output(KV.of("CameraInfo/Roi/width", Integer.toString(width.getValue().intValue())));
                                                   c.output(KV.of("CameraInfo/Roi/height", Integer.toString(height.getValue().intValue())));
                                                   c.output(KV.of("CameraInfo/Roi/y_offset", Integer.toString(y_offset.getValue().intValue())));
                                                   c.output(KV.of("CameraInfo/Roi/x_offset", Integer.toString(x_offset.getValue().intValue())));
                                                   c.output(KV.of("CameraInfo/Header/seq", Integer.toString(seq.getValue().intValue())));
                                                   c.output(KV.of("CameraInfo/Header/frame_id", frame_id.getValue()));
                                                   c.output(KV.of("CameraInfo/Header/time", Long.toString(time.getValue().getTime())));


                                               } catch (UninitializedFieldException e) {
                                                   e.printStackTrace();
                                               }
                                               System.out.println(roi.getFieldNames().toString());
                                           }


                                           return true;
                                       }
                                   });






                        } catch (BagReaderException e) {
                            e.printStackTrace();
                        }
                    }
                }))
                .apply("Format RosBag Data",MapElements.via(new SimpleFunction<KV<String, String>, String>() {
                    @Override
                    public String apply(KV<String, String> input) {
                        return String.format("%s: %s",input.getKey(), input.getValue());
                    }
                }))
                .apply("Write to File",TextIO.write().to("output").withNumShards(1));


        pipeline.run();
    }



    /** Specific pipeline options. */
    private interface Options extends PipelineOptions {
        @Description("Input Path")
        String getInput();

        void setInput(String value);



    }
}
