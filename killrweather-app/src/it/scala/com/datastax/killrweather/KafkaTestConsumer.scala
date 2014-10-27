package com.datastax.killrweather

import java.util.concurrent.CountDownLatch
import java.util.Properties
import java.util.concurrent.Executors

import kafka.serializer.StringDecoder
import kafka.consumer.{Consumer, ConsumerConfig}

class KafkaTestConsumer(zookeeper: String, topic: String, groupId: String, partitions: Int, numThreads: Int, latch: CountDownLatch) {

  val  consumer: kafka.consumer.ConsumerConnector = Consumer.create(createConsumerConfig)

  // create n partitions of the stream for topic “test”, to allow n threads to consume
  val topicMessageStreams = consumer.createMessageStreams(Map(topic -> partitions), new StringDecoder(), new StringDecoder())

  val streams = topicMessageStreams.get(topic)

  // launch all the threads
  val executor = Executors.newFixedThreadPool(numThreads)

  // consume the messages in the threads
  for(stream <- streams) {
    executor.submit(new Runnable() {
      def run() {
        for(s <- stream) {
          while(s.iterator.hasNext) {
            println(s"Consumer (KafkaStream) received: ${new String(s.iterator.next.message)}")
            latch.countDown()
          }
        }
      }
    })
  }

  def createConsumerConfig: ConsumerConfig = {
    val props = new Properties()
    props.put("consumer.timeout.ms", "2000")
    props.put("zookeeper.connect", zookeeper)
    props.put("group.id", groupId)
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "10")
    props.put("auto.commit.interval.ms", "1000")

    new ConsumerConfig(props)
  }

  def shutdown() {
    println("Consumer shutting down.")
    if (consumer != null) consumer.shutdown()
    if (executor != null) executor.shutdown()
  }
}