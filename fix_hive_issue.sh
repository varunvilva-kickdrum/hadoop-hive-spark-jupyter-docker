docker exec -i hive bash -c '
/opt/hive/bin/schematool -dbType postgres \
  -url "jdbc:postgresql://postgres:5432/metastore" \
  -userName hive \
  -passWord hive \
  -initSchema -verbose

nohup /opt/hive/bin/hive --service metastore > /tmp/metastore.log 2>&1 &
'