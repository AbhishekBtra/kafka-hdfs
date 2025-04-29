package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class HdfsMetadataProducer {

    private static final String HDFS_URI = "hdfs://localhost:9000";
    private static final String DIRECTORY_PATH = "/abatra/csvs/";
    private static final String KAFKA_TOPIC = "hdfs-metadata-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        Configuration hadoopConf = new Configuration();
        hadoopConf.addResource(new Path("/home/abatra/hadoop/hadoop-3.4.0/etc/hadoop/core-site.xml"));
        hadoopConf.addResource(new Path("/home/abatra/hadoop/hadoop-3.4.0/etc/hadoop/hdfs-site.xml"));
        hadoopConf.set("fs.defaultFS", HDFS_URI);
        FileSystem fs = FileSystem.get(hadoopConf);

        ObjectMapper mapper = new ObjectMapper();
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps);

        long now = System.currentTimeMillis();
        long fifteenMinutesAgo = now - (15 * 60 * 1000);

        FileStatus[] files = fs.listStatus(new Path(DIRECTORY_PATH));

        for (FileStatus file : files) {
            if (!file.isFile()) continue;

            long modTime = file.getModificationTime();
            if (modTime >= fifteenMinutesAgo) {
                Map<String, Object> metadata = new HashMap<>();
                metadata.put("path", file.getPath().toString());
                metadata.put("size", file.getLen());
                metadata.put("modification_time", modTime);
                metadata.put("owner", file.getOwner());
                metadata.put("group", file.getGroup());

                String jsonMetadata = mapper.writeValueAsString(metadata);

                ProducerRecord<String, String> record = new ProducerRecord<>(KAFKA_TOPIC, file.getPath().getName(), jsonMetadata);
                producer.send(record);

                System.out.println("Sent: " + jsonMetadata);
            }
        }

        producer.close();
        fs.close();
    }
}
