package com.gcp.cookbook.beam.recipes.parsers;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.swrirobotics.bags.reader.BagFile;
import com.github.swrirobotics.bags.reader.BagReader;
import com.github.swrirobotics.bags.reader.MessageHandler;
import com.github.swrirobotics.bags.reader.TopicInfo;
import com.github.swrirobotics.bags.reader.exceptions.BagReaderException;
import com.github.swrirobotics.bags.reader.exceptions.UninitializedFieldException;
import com.github.swrirobotics.bags.reader.messages.serialization.*;
import com.github.swrirobotics.bags.reader.records.Connection;

import java.util.List;
import java.util.ListIterator;

import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class ReadRosBag {
    private static final Logger LOG = LoggerFactory.getLogger(ReadRosBag.class);


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
                .apply("Map File to Record strings[]", MapElements.into(strings()).via(new GetRecordsFn()))
                 .apply (ParDo.of(new DoFn<String, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        LOG.info(c.element().toString());
                    }
                }));

        pipeline.run();
    }

    private static class GetRecordsFn implements SerializableFunction<FileIO.ReadableFile, String> {


        @Override
        public String apply(FileIO.ReadableFile input) {

            final String[] frameId = {""};
            BagFile file = null;
            try {
                file = BagReader.readFile(input.getMetadata().resourceId().toString());
            } catch (BagReaderException e) {
                e.printStackTrace();
            }

            try {

                file.forMessagesOfType("sensor_msgs/Image", new MessageHandler() {
                    @Override
                    public boolean process(MessageType message, Connection connection) {
                        //System.out.println(message.getFieldNames());
                        ListIterator<String> litr = null;
                        // String data = message.<StringType>getField("").getValue();
                        //System.out.println("Field "+ message.getFieldNames());



                        MessageType header = message.getField( "header");

                        List<String> names = null;
                        names = header.getFieldNames();
                        litr=names.listIterator();

                        //System.out.println("Traversing the list in forward direction: for ");
                        while(litr.hasNext()){
                            litr.next();
                            try {
                                //System.out.println(header.<StringType>getField("frame_id").getValue());
                                if(frameId[0]!="")
                                    frameId[0] = frameId[0] + "," + header.<StringType>getField("frame_id").getValue().toString();
                                else
                                    frameId[0] =  header.<StringType>getField("frame_id").getValue().toString();

                            } catch (UninitializedFieldException e) {
                                e.printStackTrace();
                            }
                        }


                        return true;
                    }


                });
            } catch (BagReaderException e) {
                e.printStackTrace();
            }



            return frameId[0].toString();
        }
    }

    /** Specific pipeline options. */
    private interface Options extends PipelineOptions {
        @Description("Input Path")
        String getInput();

        void setInput(String value);


        @Description("RosBag Topic Name")
        @Default.String("sensor_msgs/Image")
        String getTopicname();
        void setTopicname(String topicName);
    }
}
