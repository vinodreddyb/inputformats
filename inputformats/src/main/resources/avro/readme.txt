
http://java.dzone.com/articles/mapreduce-avro-data-files

1. Genreate schema to java
   java -jar avro-tools-1.7.7.jar compile schema /home/vinod/git/inputformat/inputformats/src/main/resources/avro/student.avsc .
2. java -jar avro-tools-1.7.7.jar fromjson /home/vinod/git/inputformat/inputformats/src/main/resources/avro/student.json --schema-file /home/vinod/git/inputformat/inputformats/src/main/resources/avro/student.avsc > /home/vinod/git/inputformat/inputformats/src/main/resources/avro/student.avro
    hadoop fs -put /home/vinod/git/inputformat/inputformats/src/main/resources/avro/student.avro /input/avro/schema

3. To view output

   java -jar avro-tools-1.7.7.jar tojson /home/vinod/work/output/avro/part-r-00000.avro > output.json