package com.gcp.cookbook.beam.recipes.parsers;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gcp.cookbook.beam.recipes.BeamFunctions;
import org.apache.beam.sdk.extensions.jackson.ParseJsons;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JsonParser {


    private static final Logger LOG = LoggerFactory.getLogger(JsonParser.class);

    public static void main(String[] args) {

        parseUsingCustomMapper();
        parseValidJsons();


    }


    private static final List<String> VALID_JSONS =
            Arrays.asList(
                    "{\"myString\":\"abc\",\"myInt\":3}",
                    "{\"myString\":\"def\",\"myInt\":4}"
            );




    private static final List<String> EXTRA_PROPERTIES_JSONS =
            Arrays.asList(
                    "{\"myString\":\"abc\",\"myInt\":3,\"other\":1}",
                    "{\"myString\":\"def\",\"myInt\":4}"
            );

    private static final List<MyPojo> POJOS =
            Arrays.asList(
                    new MyPojo("abc", 3),
                    new MyPojo("def", 4)
            );

    private static final List<MyEmptyBean> EMPTY_BEANS =
            Arrays.asList(
                    new MyEmptyBean("abc", 3),
                    new MyEmptyBean("def", 4)
            );


    public static void parseValidJsons() {

        Pipeline pipeline = BeamFunctions.createPipeline("Parse Json Examples");

        PCollection<MyPojo> output =
                pipeline
                        .apply(Create.of(VALID_JSONS))
                        .apply(ParseJsons.of(MyPojo.class)).setCoder(SerializableCoder.of(MyPojo.class));

        output.apply(ParDo.of(new DoFn<MyPojo, Void>() {
            // @Override

            @ProcessElement
            public void processElement(ProcessContext c)  {
                LOG.info(c.element().getMyString()  + "," + c.element().getMyInt());
            }


        }));

        pipeline.run().waitUntilFinish();
    }


    public  static void parseUsingCustomMapper() {
        ObjectMapper customMapper =
                new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        Pipeline pipeline = BeamFunctions.createPipeline("Parse Json With Custom Mapper");

        PCollection<MyPojo> output =
                pipeline
                        .apply(Create.of(EXTRA_PROPERTIES_JSONS))
                        .apply(ParseJsons.of(MyPojo.class)
                                .withMapper(customMapper)).setCoder(SerializableCoder.of(MyPojo.class));


        pipeline.run().waitUntilFinish();
    }



    /**
     * Pojo for tests.
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public static class MyPojo implements Serializable {
        private String myString;
        private int myInt;

        public MyPojo() {
        }

        public MyPojo(String myString, int myInt) {
            this.myString = myString;
            this.myInt = myInt;
        }

        public  String getMyString() {
            return myString;
        }

        public  void setMyString(String myString) {
            this.myString = myString;
        }

        public  int getMyInt() {
            return myInt;
        }

        public  void setMyInt(int myInt) {
            this.myInt = myInt;
        }

        @Override
        public  boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (!(o instanceof MyPojo)) {
                return false;
            }

            MyPojo myPojo = (MyPojo) o;

            return myInt == myPojo.myInt && (myString != null ? myString.equals(myPojo.myString) :
                    myPojo.myString == null);
        }

        @Override
        public  int hashCode() {
            int result = myString != null ? myString.hashCode() : 0;
            result = 31 * result + myInt;
            return result;
        }
    }

    /**
     * Pojo for tests.
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public static class MyEmptyBean implements Serializable {
        private String myString;
        private int myInt;

        public MyEmptyBean(String myString, int myInt) {
            this.myString = myString;
            this.myInt = myInt;
        }
    }
}
