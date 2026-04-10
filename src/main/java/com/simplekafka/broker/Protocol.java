package com.simplekafka.broker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Defines the binary wire protocol for SimpleKafka.
 * All communication between brokers and client uses this format.
 */

public class Protocol {
    
    //  --- Client Request Types ---
    public static final byte PRODUCE            = 0x01;
    public static final byte FETCH              = 0x02;
    public static final byte METADATA           = 0x03;
    public static final byte CREATE_TOPIC       = 0x04;

    // --- Broker Request Tyoe ---
    public static final byte PRODUCE_RESPONSE   = 0x11;
    public static final byte FETCH_RESPONSE     = 0x12;
    public static final byte METADATA_RESPONSE  = 0x13;
    public static final byte ERROR_RESPONSE     = 0x14;

    // --- Broker to Broker Type ---
    public static final byte REPLICATE          = 0x21;
    public static final byte TOPIC_NOTIFICATION = 0x22;

    /**
     * REQUEST ENCODERS
     */

    public static ByteBuffer encodeProduceRequest(String topic, int partition, byte[] message) {
        byte[] topicBytes = topic.getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(1 + 2 + topicBytes.length + 4 + 4 + message.length);
        buffer.put(PRODUCE);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putInt(message.length);
        buffer.put(message);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeFetchRequest(String topic, int partition, long offset, int maxBytes) {
        byte[] topicBytes = topic.getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(1 + 2 + topicBytes.length + 4 + 8 + 4);
        buffer.put(FETCH);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(maxBytes);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeMetadataRequest() {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put(METADATA);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeCreateTopicRequest(String topic, int numPartitions, short replicationFactor) {
        byte[] topicBytes = topic.getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(1 + 2 + topicBytes.length + 4 + 2);
        buffer.put(CREATE_TOPIC);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(numPartitions);
        buffer.putShort(replicationFactor);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeReplicateRequest(String topic, int partition, long offset, byte[] message) {
        byte[] topicBytes = topic.getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(1 + 2 + topicBytes.length + 4 + 8 + 4 + message.length);
        buffer.put(REPLICATE);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(message.length);
        buffer.put(message);
        buffer.flip();
        return buffer;
    }

    /**
     * Encode a topic notification(broker to broker)
     */

    public static ByteBuffer encodeTopicNotification(String topic) {
        byte[] topicBytes = topic.getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(1 + 2 + topicBytes.length);
        buffer.put(TOPIC_NOTIFICATION);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.flip();
        return buffer;
    }

    /**
     * RESPONSE DECODERS
     */
    public static ProduceResult decodeProduceResponse(ByteBuffer buffer) {
        byte responseType = buffer.get();
        if (responseType == ERROR_RESPONSE) {
            short msgLen = buffer.getShort();
            byte[] msgBytes = new byte[msgLen];
            buffer.get(msgBytes);
            return new ProduceResult(-1, new String(msgBytes));
        }
        if (responseType != PRODUCE_RESPONSE) {
            return new ProduceResult(-1, "Unexpected response type: " + responseType);
        }
        long offset = buffer.getLong();
        byte status = buffer.get();
        if (status == 0) {
            return new ProduceResult(offset, null);
        } else {
            return new ProduceResult(-1, "Produce failed with status: " + status);
        }
    }

    public static FetchResult decodeFetchResponse(ByteBuffer buffer) {
        byte responseType = buffer.get();
        if (responseType == ERROR_RESPONSE) {
            short msgLen = buffer.getShort();
            byte[] msgBytes = new byte[msgLen];
            buffer.get(msgBytes);
            return new FetchResult(null, new String(msgBytes));
        }
        if (responseType != FETCH_RESPONSE) {
            return new FetchResult(null, "Unexpected response type: " + responseType);
        }
        int messageCount = buffer.getInt();
        List<byte[]> messages = new ArrayList<>();
        for (int i = 0; i < messageCount; i++) {
            buffer.getLong();
            int msgLen = buffer.getInt();
            byte[] msg = new byte[msgLen];
            buffer.get(msg);
            messages.add(msg);
        }
        return new FetchResult(messages, null);
    }

    public static MetadataResult decodeMetadataResponse(ByteBuffer buffer) {
        byte responseType = buffer.get();
        if (responseType == ERROR_RESPONSE) {
            short msgLen = buffer.getShort();
            byte[] msgBytes = new byte[msgLen];
            buffer.get(msgBytes);
            return new MetadataResult(null, null, new String(msgBytes));
        }
        if (responseType != METADATA_RESPONSE) {
            return new MetadataResult(null, null, "Unexpected response type: " + responseType);
        }

        // Brokers
        int brokerCount = buffer.getInt();
        List<BrokerInfo> brokers = new ArrayList<>();
        for (int i = 0; i < brokerCount; i++) {
            int brokerId = buffer.getInt();
            short hostLen = buffer.getShort();
            byte[] hostBytes = new byte[hostLen];
            buffer.get(hostBytes);
            int port = buffer.getInt();
            brokers.add(new BrokerInfo(brokerId, new String(hostBytes), port));
        }

        // Topics
        int topicCount = buffer.getInt();
        List<TopicMetadata> topics = new ArrayList<>();
        for (int i = 0; i < topicCount; i++) {
            short topicLen = buffer.getShort();
            byte[] topicBytes = new byte[topicLen];
            buffer.get(topicBytes);
            String topicName = new String(topicBytes);

            int partitionCount = buffer.getInt();
            List<PartitionMetadata> partitions = new ArrayList<>();
            for (int j = 0; j < partitionCount; j++) {
                int partId     = buffer.getInt();
                int leaderId   = buffer.getInt();
                int replicaCount = buffer.getInt();
                List<Integer> replicas = new ArrayList<>();
                for (int k = 0; k < replicaCount; k++) {
                    replicas.add(buffer.getInt());
                }
                partitions.add(new PartitionMetadata(partId, leaderId, replicas));
            }
            topics.add(new TopicMetadata(topicName, partitions));
        }

        return new MetadataResult(brokers, topics, null);
    }

    /**
     * ERROR HELPER
     */
    public static void sendErrorResponse(SocketChannel channel, String errorMessage) throws IOException {
        byte[] msgBytes = errorMessage.getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(1 + 2 + msgBytes.length);
        buffer.put(ERROR_RESPONSE);
        buffer.putShort((short) msgBytes.length);
        buffer.put(msgBytes);
        buffer.flip();
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    /**
     * RESULT CLASSES
     */

    public static class ProduceResult {
        public final long offset;
        public final String error;
        public ProduceResult(long offset, String error) {
            this.offset = offset;
            this.error  = error;
        }
        public boolean isSuccess() { return error == null; }
    }

    public static class FetchResult {
        public final List<byte[]> messages;
        public final String error;
        public FetchResult(List<byte[]> messages, String error) {
            this.messages = messages;
            this.error    = error;
        }
        public boolean isSuccess() { return error == null; }
    }

    public static class MetadataResult {
        public final List<BrokerInfo>     brokers;
        public final List<TopicMetadata>  topics;
        public final String               error;
        public MetadataResult(List<BrokerInfo> brokers, List<TopicMetadata> topics, String error) {
            this.brokers = brokers;
            this.topics  = topics;
            this.error   = error;
        }
        public boolean isSuccess() { return error == null; }
    }

    public static class TopicMetadata {
        public final String                  topicName;
        public final List<PartitionMetadata> partitions;
        public TopicMetadata(String topicName, List<PartitionMetadata> partitions) {
            this.topicName  = topicName;
            this.partitions = partitions;
        }
    }

    public static class PartitionMetadata {
        public final int          partitionId;
        public final int          leaderId;
        public final List<Integer> replicaIds;
        public PartitionMetadata(int partitionId, int leaderId, List<Integer> replicaIds) {
            this.partitionId = partitionId;
            this.leaderId    = leaderId;
            this.replicaIds  = replicaIds;
        }

}
