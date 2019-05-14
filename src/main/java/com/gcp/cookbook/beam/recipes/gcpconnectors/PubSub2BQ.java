package com.gcp.cookbook.beam.recipes.gcpconnectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import com.gcp.cookbook.beam.recipes.gcpconnectors.options.PubSub2BQOptions;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class PubSub2BQ {

    private static final Logger log = LoggerFactory.getLogger(PubSub2BQ.class);

    public static void main(String[] args) {

        PubSub2BQOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSub2BQOptions.class);

        try {
            log.info("Options:");
            log.info(new ObjectMapper().writeValueAsString(options));

            if (options.getTopic() != null) {
                execute(options);
            } else {
                log.error("options.getFilePath() is NULL");
            }

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
    }


    static void execute(PubSub2BQOptions options) {

        List<TableFieldSchema> schema = new ArrayList<>();
        schema.add(new TableFieldSchema().setName("id").setType("STRING"));
        schema.add(new TableFieldSchema().setName("name").setType("STRING"));
        schema.add(new TableFieldSchema().setName("metric1").setType("NUMERIC"));
        schema.add(new TableFieldSchema().setName("metric2").setType("NUMERIC"));
        schema.add(new TableFieldSchema().setName("metric3").setType("NUMERIC"));
        schema.add(new TableFieldSchema().setName("measurement").setType("NUMERIC"));
        schema.add(new TableFieldSchema().setName("heat").setType("NUMERIC"));
        schema.add(new TableFieldSchema().setName("iot_timestamp").setType("TIMESTAMP"));

        if (options != null) {

            Pipeline p = Pipeline.create(options);

            //Create/Overwrite file
            p.apply(PubsubIO.readMessages().fromTopic(options.getTopic()))
                    // Convert PubSubMessage Body into TableRow
                    .apply(
                            MapElements
                                    .into(TypeDescriptor.of(TableRow.class))
                                    .via((SerializableFunction<PubsubMessage, TableRow>) input -> {
                                        try {
                                            log.info(new String(input.getPayload()));

                                            //parse pubsub message
                                            Map message = new ObjectMapper().readValue(input.getPayload(), Map.class);

                                            //convert to TableRow
                                            TableRow row = new TableRow();
                                            row.putAll(message);

                                            return row;
                                        } catch (IOException ex) {
                                            ex.printStackTrace();
                                        }
                                        return null;
                                    })
                    )

                    //Save Message
                    .apply(
                            BigQueryIO.writeTableRows()
                                    .withExtendedErrorInfo()
                                    .to(options.getBigQueryTablename())
                                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                    .withSchema(new TableSchema().setFields(schema))
                    );

            p.run().waitUntilFinish();

        }
    }
}
