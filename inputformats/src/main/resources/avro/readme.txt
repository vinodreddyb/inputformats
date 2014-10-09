
http://java.dzone.com/articles/mapreduce-avro-data-files
java -jar avro-tools-1.7.7.jar fromjson /home/vinod/git/inputformat/inputformats/src/main/resources/avro/student.json --schema-file /home/vinod/git/inputformat/inputformats/src/main/resources/avro/student.avsc > /home/vinod/git/inputformat/inputformats/src/main/resources/avro/student.avro
hadoop fs -put /home/vinod/git/inputformat/inputformats/src/main/resources/avro/student.avro /input/avro/schema