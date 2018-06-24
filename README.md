/opt/spark-2.3.1-bin-hadoop2.7/bin/spark-submit \
--conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8" \
--class org.huasuoworld.app.HdfsToSolr \
--master local[1] \
/data/spark/jobs/solr_job-jar-with-dependencies.jar \
"hello guys, my name is zhazhahui"! /data/spark/jobs/zhazhahui.txt