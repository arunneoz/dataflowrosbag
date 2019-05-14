package com.gcp.cookbook.beam.recipes.parsers;


import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Avro2Csv {

    private static final Logger LOG = LoggerFactory.getLogger(Avro2Csv.class);
    private static final List<String> acceptedTypes = Arrays.asList(
            new String[]{"string", "boolean", "int", "long", "float", "double"});

    private static String getSchema(String schemaPath) throws IOException {
        ReadableByteChannel channel = FileSystems.open(FileSystems.matchNewResource(
                schemaPath, false));

        try (InputStream stream = Channels.newInputStream(channel)) {
            BufferedReader streamReader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
            StringBuilder dataBuilder = new StringBuilder();

            String line;
            while ((line = streamReader.readLine()) != null) {
                dataBuilder.append(line);
            }

            return dataBuilder.toString();
        }
    }

    public static void checkFieldTypes(Schema schema) throws IllegalArgumentException {
        for (Schema.Field field : schema.getFields()) {
            String fieldType = field.schema().getType().getName().toLowerCase();
            if (!acceptedTypes.contains(fieldType)) {
                LOG.error("Data transformation doesn't support: " + fieldType);
                throw new IllegalArgumentException("Field type " + fieldType + " is not supported.");
            }
        }
    }

    public static class ConvertAvroToCsv extends DoFn<GenericRecord, String> {

        private String delimiter;
        private String schemaJson;

        public ConvertAvroToCsv(String schemaJson, String delimiter) {
            this.schemaJson = schemaJson;
            this.delimiter = delimiter;
        }

        @ProcessElement
        public void processElement(ProcessContext ctx) {
            GenericRecord genericRecord = ctx.element();
            Schema schema = new Schema.Parser().parse(schemaJson);

            StringBuilder row = new StringBuilder();
            for (Schema.Field field : schema.getFields()) {
                String fieldType = field.schema().getType().toString().toLowerCase();
                if (!acceptedTypes.contains(fieldType)) {
                    LOG.error("Data transformation doesn't support: " + fieldType);
                    throw new IllegalArgumentException("Field type " + fieldType + " is not supported.");
                }
                if (row.length() > 0) {
                    row.append(delimiter);
                }

                row.append(genericRecord.get(field.name()));
            }
            ctx.output(row.toString());
        }
    }

    public static void runAvroToCsv(SampleOptions options)
            throws IOException, IllegalArgumentException {
        FileSystems.setDefaultPipelineOptions(options);

        // Get Avro Schema
        String schemaJson = getSchema(options.getAvroSchema());
        Schema schema = new Schema.Parser().parse(schemaJson);

        // Check schema field types before starting the Dataflow job
        checkFieldTypes(schema);

        // Create the Pipeline object with the options we defined above.
        Pipeline pipeline = Pipeline.create(options);

        // Convert Avro To CSV
        pipeline.apply("Read Avro files",
                AvroIO.readGenericRecords(schemaJson).from(options.getInputFile()))
                .apply("Convert Avro to CSV formatted data",
                        ParDo.of(new ConvertAvroToCsv(schemaJson, options.getCsvDelimiter())))
                .apply("Write CSV formatted data", TextIO.write().to(options.getOutput())
                        .withSuffix(".csv"));

        // Run the pipeline.
        pipeline.run().waitUntilFinish();
    }

    public static void main(String[] args) throws IOException, IllegalArgumentException {
        // Create and set your PipelineOptions.
        PipelineOptionsFactory.register(SampleOptions.class);
        SampleOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(SampleOptions.class);

        runAvroToCsv(options);
    }



    public interface SampleOptions extends DataflowPipelineOptions {

        /**
         * Set inuptFile required parameter to a file path or glob. A local path and Google Cloud Storage
         * path are both supported.
         */
        @Description(
                "Set inuptFile required parameter to a file path or glob. A local path and Google Cloud "
                        + "Storage path are both supported.")
        @Validation.Required
        String getInputFile();

        void setInputFile(String value);

        /**
         * Set output required parameter to define output path. A local path and Google Cloud Storage path
         * are both supported.
         */
        @Description(
                "Set output required parameter to define output path. A local path and Google Cloud Storage"
                        + " path are both supported.")
        @Validation.Required
        String getOutput();

        void setOutput(String value);

        /**
         * Set avroSchema required parameter to specify location of the schema. A local path and Google
         * Cloud Storage path are supported.
         */
        @Description(
                "Set avroSchema required parameter to specify location of the schema. A local path and "
                        + "Google Cloud Storage path are supported.")
        @Validation.Required
        String getAvroSchema();

        void setAvroSchema(String value);

        /**
         * Set csvDelimiter optional parameter to specify the CSV delimiter. Default delimiter is set to a
         * comma.
         */
        @Description(
                "Set csvDelimiter optional parameter to specify the CSV delimiter. Default delimiter is set"
                        + " to a comma.")
        @Default.String(",")
        String getCsvDelimiter();

        void setCsvDelimiter(String delimiter);
    }

}