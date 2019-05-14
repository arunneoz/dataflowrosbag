package com.gcp.cookbook.beam.recipes.gcpconnectors.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface PubSub2BQOptions extends PipelineOptions {

    @Description("Topic")
        //mock-iot-stream
    ValueProvider<String> getTopic();
    void setTopic(ValueProvider<String> value);


    @Description("Big Query table name")
    @Default.String("iot measurements")
    String getBigQueryTablename();
    void setBigQueryTablename(String bigQueryTablename);


}
