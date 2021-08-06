# Databricks notebook source
# MAGIC %python
# MAGIC from dateutil import parser
# MAGIC import time
# MAGIC import copy
# MAGIC import math
# MAGIC import pyspark
# MAGIC import math
# MAGIC from delta.tables import *
# MAGIC from pyspark.sql import SparkSession
# MAGIC from pyspark.sql.functions import *
# MAGIC from delta.tables import *
# MAGIC import haversine as hs
# MAGIC from datetime import datetime, timedelta
# MAGIC from datetime import datetime, timedelta
# MAGIC import json
# MAGIC from datetime import datetime, timedelta
# MAGIC from pyspark.sql.functions import udf,array

# COMMAND ----------

spark.sql("use {}".format("ccd_db_test"))

# COMMAND ----------

subscription_type_status_map = {}
subscription_type_status_map['EtaAlertSubscription'] = 'At Destination Hub' 
subscription_type_status_map['ParcelShippingDelayAlertSubscription'] = 'Available for pickup'


# COMMAND ----------



# COMMAND ----------

#subscription_for_alert_types = ["ParcelDestinationHubDelayAlertSubscription", "ParcelShippingDelayAlertSubscription"]

subscriptions_query = spark.sql("select s.id as sub_id, s.type as sub_type, s.settings as sub_set, s.user_id as sub_user from alert_subscriptions s where s.type ='EtaAlertSubscription' or s.type='ParcelShippingDelayAlertSubscription' limit 100");
subscriptions = subscriptions_query.distinct()  #subscriptions is dataframe here
# subscriptions = subscriptions.collect()  #creating list
# subscriptions

# COMMAND ----------

def time_status(z):
  json_set = json.loads(z)
  time_threshold = json_set['time']
  time_hour_threshold=int(float(time_threshold))
  time_min_threshold=int((float(time_threshold)-int(float(time_threshold)))*60)
  d = datetime.today() - timedelta(hours=time_hour_threshold, minutes=time_min_threshold)
  return d.strftime("%d/%m/%Y %H:%M:%S")
  
time_udf=udf(lambda z:time_status(z))

# COMMAND ----------

def trans(z,mapping):
  return mapping[z]

def translate(mapping):
  return udf(lambda z: trans(z,mapping))

# COMMAND ----------

# subscriptions.show()
result=subscriptions.withColumn("time_before_status_change",time_udf(subscriptions['sub_set'])).withColumn("status",translate(subscription_type_status_map)("sub_type"))
# result.printSchema()
result.createOrReplaceTempView('alert_result')
alert_info = spark.sql("select (sh.id, sh.parcel_digest) as info,ar.sub_type as shipment_type,ar.sub_id as shipment_id from alert_result ar inner join users u on u.id = ar.sub_user inner join flows f on u.id = f.primary_user_id inner join shipments sh on f.id = sh.flow_id where sh.milestone_carrier_status = ar.status and sh.milestone_event_time < ar.time_before_status_change").show()


# COMMAND ----------

alert_info.write
  .format("kafka")
  .option("kafka.bootstrap.servers", server)
  .save()