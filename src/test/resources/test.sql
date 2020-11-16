CREATE TABLE kudu.test.t10
(
    id int WITH (primary_key = true),
    t1 timestamp,
    t2 timestamp
)
WITH (
    partition_by_hash_columns = ARRAY['id'],
    partition_by_hash_buckets = 10
);

java -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=10003 -jar debezium-to-kudu.jar --spring.profiles.active=prod