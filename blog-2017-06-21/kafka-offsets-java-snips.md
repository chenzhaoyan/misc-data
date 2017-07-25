### Purpose

This page is solely intended to provide a java code example for storing kakfa offsets in ZooKeeper.
See blog for initialization of ZooKeeper client. Also, remember to close the connection upon termination.

__Blog reference:__
[Offset Management For Apache Kafka With Apache Spark Streaming](http://blog.cloudera.com/blog/2017/06/offset-management-for-apache-kafka-with-apache-spark-streaming/#comments, "Offset Management For Apache Kafka With Apache Spark Streaming")

### Code snips
##### Method for retrieving the last offsets stored in ZooKeeper of the consumer group and topic list
    /**
     * Retrieve offsets stored in zk of the consumer group and topic list
     * @param topics
     * @param groupId
     * @return
     */
    public Map<TopicAndPartition, Long> readOffsets(List<String> topics, String groupId) {
      Map<TopicAndPartition, Long> topicPartOffsetMap = new HashMap<>();
      Seq<String> topicSeq = JavaConversions.asScalaBuffer(topics).seq();
      scala.collection.mutable.Map partitionsForTopics = zkUtils.getPartitionsForTopics(topicSeq);
      Map<String, Seq<Integer>> partitionMap = JavaConversions.mapAsJavaMap(partitionsForTopics);

      // /consumers/<groupId>/offsets/<topic>/
      partitionMap.keySet().forEach(topic -> {
        ZKGroupTopicDirs zkGroupTopicDirs = new ZKGroupTopicDirs(groupId, topic.toString());
        List<Integer> partitions = JavaConversions.seqAsJavaList(partitionMap.get(topic.toString()));

        partitions.forEach(p -> {
          String offsetPath = zkGroupTopicDirs.consumerOffsetDir() + "/" + p.toString();

          try {
            Tuple2<String, Stat> offsetStatTuple = zkUtils.readData(offsetPath);
            if (offsetStatTuple != null) {
              LOGGER.info("retrieving offset details - topic: {}, partition: " +
                  "{}, offset: {}, node path: {}",
                topic, p, offsetStatTuple._1(), offsetPath);

              topicPartOffsetMap.put(new TopicAndPartition(topic.toString(), Integer.valueOf(p.toString())),
                Long.valueOf(offsetStatTuple._1()));
            }
          } catch (Exception e) {
            LOGGER.warn("retrieving offset details - no previous node exists: {}, topic: {}, partition: {}, " +
                "node path: {}", e.getMessage(), topic, p, offsetPath);

            topicPartOffsetMap.put(new TopicAndPartition(topic.toString(), Integer.valueOf(p.toString())), 0L);
          }
        });
      });
      return topicPartOffsetMap;
    }

##### Method for persisting a recoverable set of offsets to ZooKeeper
    /**
    * Persist a recoverable checkpoint of processed data to zk
    *
    * @param offsetRanges
    * @param groupId
    * @param storeEndOffset
    * @throws InterruptedException
    */
    public void persistOffsets(OffsetRange[] offsetRanges, String groupId,
      boolean storeEndOffset) throws InterruptedException {
      for (OffsetRange or : offsetRanges) {
        ZKGroupTopicDirs zkGroupTopicDirs = new ZKGroupTopicDirs(groupId, or.topic());

        List<ACL> acls = new ArrayList<>();
        ACL acl = new ACL();
        acl.setId(ANYONE_ID_UNSAFE);
        acl.setPerms(PERMISSIONS_ALL);
        acls.add(acl);

        String offsetPath = zkGroupTopicDirs.consumerOffsetDir() + "/" + or.partition();
        long offsetVal = storeEndOffset ? or.untilOffset() : or.fromOffset();
        zkUtils.updatePersistentPath(zkGroupTopicDirs.consumerOffsetDir() + "/" + or.partition(), offsetVal + "", acls);
        LOGGER.debug("persisting offset details - topic: {}, partition: {}, offset: {}, node path: {}", or.topic(), 
          or.partition(), offsetVal, offsetPath);
      }
    }

