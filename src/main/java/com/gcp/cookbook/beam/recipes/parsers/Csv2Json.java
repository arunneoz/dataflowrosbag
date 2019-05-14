package com.gcp.cookbook.beam.recipes.parsers;




import com.gcp.cookbook.beam.recipes.BeamFunctions;
import org.apache.beam.sdk.Pipeline;

import org.apache.beam.sdk.io.TextIO;

import com.gcp.cookbook.beam.recipes.parsers.model.Person;



import org.apache.beam.sdk.transforms.*;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;



public class Csv2Json {

    private static final Logger LOG = LoggerFactory.getLogger(Csv2Json.class);


    public static void main(final String[] args) {


        Pipeline p = BeamFunctions.createPipeline("CSV 2 Json Examples");
        p.apply("ReadCSV", TextIO.read().from("/Users/asanthan/Documents/person.csv"))
                .apply("MapToPerson", ParDo.of(new DoFn<String, Person>() {
                    @ProcessElement
                    public void onElement(
                            @Element final String file,
                            final OutputReceiver<Person> emitter) {
                        final String[] parts = file.split(",");
                        emitter.output(new Person(Integer.parseInt(parts[0]), parts[1],parts[2],Integer.parseInt(parts[3]),new BigDecimal(parts[4])));
                    }
                }))
                .apply("MapToJson", ParDo.of(new DoFn<Person, String>() {
                    private transient Jsonb jsonb;

                    @ProcessElement
                    public void onElement(
                            @Element final Person person,
                            final OutputReceiver<String> emitter) {
                        if (jsonb == null) {
                            jsonb = JsonbBuilder.create();
                        }
                        System.out.println(" Retrieved Values " + person.getAge());
                        emitter.output(jsonb.toJson(person));
                    }

                    @Teardown
                    public void onTearDown() {
                        if (jsonb != null) {
                            try {
                                jsonb.close();
                            } catch (final Exception e) {
                                throw new IllegalStateException(e);
                            }
                        }
                    }
                }))
                .apply("WriteToJson", TextIO.write().to("/Users/asanthan/Documents/output.json"));

      p.run().waitUntilFinish();
    }

}
