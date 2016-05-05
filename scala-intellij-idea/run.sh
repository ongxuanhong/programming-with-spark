spark-submit \
--conf 'spark.executor.extraJavaOptions=-Dconfig.fuction.conf' \
--conf 'spark.driver.extraJavaOptions=-Dconfig.file=./dev.conf' \
--conf 'spark.local.dir=/backup/spark' \
--driver-memory 3g \
spark_aggregate_os.jar \
--startDate "2016-04-09 08:00:00" \
--endDate "2016-04-09 09:00:00" \
--widgetIds "knxad_knx2991_201509231117,knxad_star1_20150930190104"