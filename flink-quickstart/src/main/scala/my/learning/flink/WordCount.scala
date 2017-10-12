package my.learning.flink

/**
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

import org.apache.flink.api.scala._
import java.util.Properties

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over some sample data
 *
 * This example shows how to:
 *
 *   - write a simple Flink program.
 *   - use Tuple data types.
 *   - write and use user-defined functions.
 */
object WordCount {
  def main(args: Array[String]) {

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // get input data
    val text = env.fromElements("To be, or not to be,--that is the question:--",
      "Whether 'tis nobler in the mind to suffer", "The slings and arrows of outrageous fortune",
      "Or to take arms against a sea of troubles, blah blah and blah")

    val counts = text.flatMap { _.toLowerCase.split("\\W+") }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    // execute and print result
    counts.print()
    setupKafka()
  }
  
  def setupKafka(){
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    println("Setting up kafka")
    val properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    // only required for Kafka 0.8
    //properties.setProperty("zookeeper.connect", "localhost:2181");
    //properties.setProperty("group.id", "test");
    val myConsumer = new FlinkKafkaConsumer010[String]("test1", new SimpleStringSchema(), properties)
    val messageStream = env.addSource(myConsumer)
    messageStream.print
    val counts = messageStream
      .map { (_, 1) }
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)
    counts.print
    env.execute("Flink Kafka Example")

  }


}
