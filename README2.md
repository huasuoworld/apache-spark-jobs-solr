/opt/spark-2.3.1-bin-hadoop2.7/bin/spark-submit \
--conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8" \
--class org.huasuoworld.app.HdfsToSolr \
--master local[1] \
/data/spark/jobs/solr_job-jar-with-dependencies.jar \
"hello guys, my name is zhazhahui"!

#往docker内添加文件
#docker cp /data/spark/jobs/solr_job-jar-with-dependencies.jar my_solr:/opt/solr/solr_job-jar-with-dependencies.jar
#删除docker 内文件
#docker exec my_solr rm -rf /opt/solr/solr_job-jar-with-dependencies.jar

#local deploy
#/opt/spark-2.3.1-bin-hadoop2.7/bin/spark-submit \

#docker run -it docker.io/mesosphere/spark bin/spark-submit \