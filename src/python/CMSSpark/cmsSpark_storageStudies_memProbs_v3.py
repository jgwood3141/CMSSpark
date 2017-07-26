#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
# Authors: Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
#          John Wood < jgwood AT physics [DOT] ucsd [DOT] edu > 
"""
Spark script to parse DBS and Xrootd records on HDFS.
"""

# system modules
import os
import re
import sys
import gzip
import time
import datetime
import json
import argparse
from types import NoneType

from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

# CMSSpark modules
from CMSSpark.spark_utils import dbs_tables, phedex_tables_multiday, jm_tables_multiday, print_rows, unionAll, files
from CMSSpark.spark_utils import spark_context, aaa_tables, split_dataset
from CMSSpark.utils import elapsed_time


# Globals, because they wont ever change
dt_6mth = 60*60*24*30*6
dt_5mth = 60*60*24*30*5
dt_4mth = 60*60*24*30*4
dt_3mth = 60*60*24*30*3
dt_2mth = 60*60*24*30*2
dt_1mth = 60*60*24*30*1
    

class OptionParser():
    def __init__(self):
        "User based option parser"
        desc = "Spark script to process DBS+JobMonitoring metadata"
        self.parser = argparse.ArgumentParser(prog='PROG', description=desc)
        year = time.strftime("%Y", time.localtime())
        hdir = 'hdfs:///project/awg/cms'
        msg = 'Location of CMS folders on HDFS, default %s' % hdir
        self.parser.add_argument("--hdir", action="store",
            dest="hdir", default=hdir, help=msg)
        fout = 'storage_datasets.csv'
        self.parser.add_argument("--fout", action="store",
            dest="fout", default=fout, help='Output file name, default %s' % fout)
        self.parser.add_argument("--fromdate", action="store",
            dest="fromdate", default="", help='Select CMSSW data for date to begin (YYYYMMDD)')
        self.parser.add_argument("--todate", action="store",
            dest="todate", default="", help='Select CMSSW data for date to end (YYYYMMDD)')
        self.parser.add_argument("--maxtime", action="store",
            dest="maxtime", default=20, help='Select maximum time window for parsing phedex and jobMontioring in days')
        msg= 'DBS instance on HDFS: global (default), phys01, phys02, phys03'
        self.parser.add_argument("--inst", action="store",
            dest="inst", default="global", help=msg)
        self.parser.add_argument("--no-log4j", action="store_true",
            dest="no-log4j", default=False, help="Disable spark log4j messages")
        self.parser.add_argument("--yarn", action="store_true",
            dest="yarn", default=False, help="run job on analytics cluster via yarn resource manager")
        self.parser.add_argument("--verbose", action="store_true",
            dest="verbose", default=False, help="verbose output")


def aaa_date(date):
    "Convert given date into AAA date format"
    if  not date:
        date = time.strftime("%Y/%m/%d", time.gmtime(time.time()-60*60*24))
        return date
    if  len(date) != 8:
        raise Exception("Given date %s is not in YYYYMMDD format" % date)
    year = date[:4]
    month = date[4:6]
    day = date[6:]
    return '%s/%s/%s' % (year, month, day)


def aaa_date_unix(date):
    "Convert AAA date into UNIX timestamp"
    return time.mktime(time.strptime(date, '%Y/%m/%d'))


def check_date_range(begin, end):
    "Check for valid date range"
    begin_unix = aaa_date_unix(begin)
    end_unix = aaa_date_unix(end)
    diff_unix = end_unix-begin_unix
    if diff_unix < 0:
        raise Exception("Given date range must have end date occur after begin date")
    return True


def jm_date(date):
    "Convert given date into JobMonitoring date format"
    if  not date:
        date = time.strftime("year=%Y/month=%-m/date=%d", time.gmtime(time.time()-60*60*24))
        return date
    if  len(date) != 8:
        raise Exception("Given date %s is not in YYYYMMDD format" % date)
    year = date[:4]
    month = int(date[4:6])
    day = int(date[6:])
    return 'year=%s/month=%s/day=%s' % (year, month, day)


def jm_date_unix(date):
    "Convert JobMonitoring date into UNIX timestamp"
    return time.mktime(time.strptime(date, 'year=%Y/month=%m/day=%d'))


def get_future_date(date, interval=1):
    "Convert given date into future date, YYYYMMDD format, interval is number of days"
    if  not date:
        date = time.strftime("YYYYmmdd", time.gmtime(time.time()-60*60*24*2))
        return date
    if  len(date) != 8:
        raise Exception("Given date %s is not in YYYYMMDD format" % date)
    return time.strftime("YYYYmmdd", datetime.date(date[:4],date[4:6],date[6:])+datetime.timedelta(days=interval))


def get_date_list(fromdate, todate, max_time_days):
    date_from = datetime.date( int(fromdate[:4]),int(fromdate[4:6]),int(fromdate[6:]) )
    date_to   = datetime.date( int(todate[:4]),  int(todate[4:6]),  int(todate[6:]) )
    max_time  = datetime.timedelta(days=max_time_days)
    if (date_to-date_from)<=max_time:
        date_list = [ (date_from.strftime("%Y%m%d"), date_to.strftime("%Y%m%d")) ]
    else:
        date_list = []
        current_start_time = date_from
        current_end_time   = date_from
        while current_end_time<date_to:
            if (current_start_time+max_time)>date_to:
                current_end_time = date_to;
            else:
                current_end_time = current_start_time + max_time;
            date_list.append( (current_start_time.strftime("%Y%m%d"), current_end_time.strftime("%Y%m%d")) )
            current_start_time += (max_time + datetime.timedelta(days=1))
    return date_list


def schema_agg_sources():
    """
    DBS DATASET_AGG_SOURCES table schema
    :returns: StructType consisting StructField array
    """
    return StructType([
             StructField("dataset", StringType(), True),
             StructField("site", StringType(), True),
             StructField("nJobs", IntegerType(), True),
             StructField("pbr_size", DoubleType(), True),
             StructField("first_job_begin", DoubleType(), True),
             StructField("last_job_begin", DoubleType(), True)
           ])


def run_agg_sources(ctx,sqlContext,fromdate, todate, maxtime, fout, yarn=None, verbose=None, inst='GLOBAL'):
    """
    Main function to run pyspark job. It requires a schema file, an HDFS directory
    with data and optional script with mapper/reducer functions.
    """
    
    # input sanitation, check time delta between fromdate and todate
    if len(fromdate) != 8:
        raise Exception("Given fromdate %s is not in YYYYMMDD format" %s)
    if len(todate) != 8:
        raise Exception("Given todate %s is not in YYYYMMDD format" %s)
    

    # define spark context, it's main object which allow to communicate with spark
    #ctx = spark_context('cms', yarn, verbose)
    #sqlContext = HiveContext(ctx)
    
    # Get list of time windows to loop over
    date_list = get_date_list(fromdate, todate, max_time_days=maxtime)
    print("### date_list: ", date_list)
    

    # read DBS tables, has info about datasets
    tables = {}
    tables.update(dbs_tables(sqlContext, inst=inst, verbose=verbose))
    dbs_d_df = tables['ddf'] # dataset table
    dbs_f_df = tables['fdf'] # file table
    

    # Loop over date pairs, and form dictionary of dataframes for job monitoring
    for date_pair in date_list:

        # Get start and end dates for this interval
        start_date = date_pair[0]
        end_date   = date_pair[1]
        print("### Grabbing phedex and jobMonitoringTables for: ", date_pair )
        
        # read Phedex tables, has info about where datasets are stored, and size of blocks
        ph_fromdate = "%s-%s-%s" % (start_date[:4], start_date[4:6], start_date[-2:])
        ph_todate   = "%s-%s-%s" % (end_date[:4],   end_date[4:6],   end_date[-2:])
        tables.update(phedex_tables_multiday(sqlContext, verbose=verbose, fromdate=ph_fromdate, todate=ph_todate))
        ph_df       = tables['phedex_df']
       
        # add sortable date columns to phedex records, account for byte -> pb conversion
        ph_df = ph_df.withColumn("now_day", F.date_format(ph_df["now_sec"].cast('timestamp'),"yyyyMMdd"))
        ph_df_records = ph_df.take(1)
        if verbose:
            print("### phedex df records", ph_df_records, type(ph_df_records))

        # aggregate phedex information, to get dataset size for each site, for each day
        ph_cols   = ['now_day', 'node_name', 'dataset_name', 'dataset_is_open', 'block_bytes']
        ph_df_agg_v0 = ph_df.select(ph_cols)\
                            .groupBy(['now_day', 'node_name', 'dataset_name', 'dataset_is_open'])\
                            .agg( F.sum('block_bytes').alias('pbr_size') )
        
        # Aggregate accross days, getting avg size for each dataset
        ph_cols   = ['node_name', 'dataset_name', 'dataset_is_open', 'pbr_size']
        ph_df_agg = ph_df_agg_v0.select(ph_cols)\
                                .groupBy(['node_name', 'dataset_name', 'dataset_is_open'])\
                                .agg( F.avg('pbr_size').alias('avg_size') )
 
        ph_df_agg.registerTempTable('ph_df_agg')
        print_rows(ph_df_agg, "ph_df_agg", verbose)
        ph_agg_records = ph_df_agg.take(1)
        if verbose:
            print("### phedex agg records", ph_agg_records, type(ph_agg_records))

        # read job monitoring, avro rdd file for a given date
        jm_fromdate = jm_date(start_date)
        jm_todate   = jm_date(end_date)
        tables.update(jm_tables_multiday(ctx, sqlContext, fromdate=jm_fromdate, todate=jm_todate, verbose=verbose))
        jm_df       = tables['jm_df']
        
        # add sortable column to job monitoring records, note jobMonitoring timeStamp in miliseconds, convert to seconds
        jm_df = jm_df.withColumn("job_day",        F.date_format( (F.col("StartedRunningTimeStamp")*0.001).cast('timestamp'),"yyyyMMdd"))\
                     .withColumn("job_begin_sec", (F.col("StartedRunningTimeStamp")*0.001) )
        
        jm_df.registerTempTable('jm_df')
        print_rows(jm_df, "jm_df", verbose)
        jm_records  = jm_df.take(1)
        if verbose:
            print("### jm records", jm_records, type(jm_records))
        
        # read AAA file for a given date
        #date = aaa_date(date)
        #tables.update(aaa_tables(sqlContext, date=date, verbose=verbose))
        #aaa_df = tables['aaa_df'] # aaa table
        #aaa_df_labeled = aaa_df.withColumn('source_tag_aaa',F.lit('aaa'))
        #aaa_df_labeled.registerTempTable('aaa_df_labeled')

        # merge DBS, PhEDEx, and AAA data
        #cols = ['node_name','d_dataset','file_lfn','pbr_size','dataset_is_open','max_replica_time','source_tag_aaa','start_time','end_time']
        #stmt =  'SELECT %s FROM ddf ' % ','.join(cols) 
        #stmt += 'JOIN fdf ON ddf.d_dataset_id = fdf.f_dataset_id ' # JOIN DBS dataset and file dfs
        #stmt += 'JOIN ph_df_agg ON ddf.d_dataset = ph_df_agg.dataset_name ' # JOIN ph and DBS dfs
        #stmt += 'JOIN aaa_df_labeled ON (fdf.f_logical_file_name = aaa_df_labeled.file_lfn AND ph_df_agg.node_name = aaa_df_labeled.server_site) ' # JOIN aaa and phedex 
            
        # Columns for JOINS
        cols = ['ddf.d_dataset AS dataset',
                'ph_df_agg.node_name AS site',
                'ph_df_agg.avg_size AS size',
                'jm_df.FileName AS job_fileName',
                'jm_df.JobId AS job_id',
                'jm_df.job_begin_sec AS job_begin'
                ]
        stmt  = 'SELECT DISTINCT %s FROM jm_df ' % ','.join(cols) # Select distinct, found duplicate jobs
        stmt += 'JOIN fdf ON fdf.f_logical_file_name = jm_df.FileName ' # Join jobMonitoring to DBS file dataframes based on fileName
        stmt += 'JOIN ddf ON ddf.d_dataset_id = fdf.f_dataset_id ' # Join DBS file and dataset dataframes based on datasetID
        stmt += 'JOIN ph_df_agg ON (ph_df_agg.node_name=jm_df.SiteName AND ddf.d_dataset=ph_df_agg.dataset_name) ' # Join phedex dataframe to jobMonitoring 
        stmt += 'WHERE jm_df.Type = "analysis" ' # Select on analysis type jobs
        join_df = sqlContext.sql(stmt)
        print_rows(join_df, stmt, verbose)
    
        # aggregate dataframe
        join_df_agg = join_df.orderBy('job_begin')\
                             .groupBy(['dataset','site'])\
                             .agg( F.count('job_id').alias('nJobs'),
                                   F.avg('size').alias('pbr_size'),
                                   F.first('job_begin').alias('first_job_begin'),
                                   F.last('job_begin').alias('last_job_begin')     )
        
        # Keep table around                 
        join_df_agg.persist(StorageLevel.MEMORY_AND_DISK)

        # write out results back to HDFS, the fout parameter defines area on HDFS
        # it is either absolute path or area under /user/USERNAME
        if  fout:
            print("### Writing out source aggregation table for %s to %s" % (start_date, end_date) )
            fout_temp = "%s/agg_sources_%s_%s/" % (fout,start_date,end_date)
            join_df_agg.write.format("com.databricks.spark.csv")\
                       .option("header", "true").save(fout_temp)

        
    # stop sparky
    #ctx.stop()

            
def run_analyze_agg(ctx,sqlContext,fromdate, todate, maxtime, fout, yarn=None, verbose=None, inst='GLOBAL'):        
    """
    Main function to run pyspark job. It requires a schema file, an HDFS directory
    with data and optional script with mapper/reducer functions.
    """
    
    # input sanitation, check time delta between fromdate and todate
    if len(fromdate) != 8:
        raise Exception("Given fromdate %s is not in YYYYMMDD format" %s)
    if len(todate) != 8:
        raise Exception("Given todate %s is not in YYYYMMDD format" %s)
    

    # define spark context, it's main object which allow to communicate with spark
    #ctx = spark_context('cms', yarn, verbose)
    #sqlContext = HiveContext(ctx)
    
    # Get list of time windows to loop over
    date_list = get_date_list(fromdate, todate, max_time_days=maxtime)
    print("### date_list: ", date_list)

    f_input = []
    for date_pair in date_list:
        temp_path = fout
        if fout[-1]!="/":
            temp_path += "/"
        f_input.append("%sagg_sources_%s_%s/part*" % (temp_path,date_pair[0],date_pair[1]))
    
    # Read aggregated dataframes
    join_df = unionAll([sqlContext.read.format('com.databricks.spark.csv')
                        .options(header='true',treatEmptyValuesAsNulls='true', nullValue='null')\
                        .load(path, schema = schema_agg_sources()) \
                        for path in f_input])

    
    # Use Window function to calculate Delta T between accesses on dataset
    w_deltaT = Window.partitionBy('dataset','site').orderBy("first_job_begin")

    # Delta t calculation
    f_deltaT  = F.col("first_job_begin").cast("long") - F.lag("last_job_begin", 1).over(w_deltaT).cast("long")
        
    # Check if Delta_t is greater than some time window
    f_check_dt_6mth = F.when( f_deltaT>dt_6mth, F.col("pbr_size") ).otherwise(0.0)
    f_check_dt_5mth = F.when( f_deltaT>dt_5mth, F.col("pbr_size") ).otherwise(0.0)
    f_check_dt_4mth = F.when( f_deltaT>dt_4mth, F.col("pbr_size") ).otherwise(0.0)
    f_check_dt_3mth = F.when( f_deltaT>dt_3mth, F.col("pbr_size") ).otherwise(0.0)
    f_check_dt_2mth = F.when( f_deltaT>dt_2mth, F.col("pbr_size") ).otherwise(0.0)
    f_check_dt_1mth = F.when( f_deltaT>dt_1mth, F.col("pbr_size") ).otherwise(0.0)
    
    # Calc deletion date for different time frames
    #f_del_date_6mth = F.when( f_deltaT>dt_6mth, F.lag("job_begin_last", 1).over(w_deltaT)+dt_6mth).otherwise(-1)
    #f_del_date_5mth = F.when( f_deltaT>dt_5mth, F.lag("job_begin_last", 1).over(w_deltaT)+dt_5mth).otherwise(-1)
    #f_del_date_4mth = F.when( f_deltaT>dt_4mth, F.lag("job_begin_last", 1).over(w_deltaT)+dt_4mth).otherwise(-1)
    #f_del_date_3mth = F.when( f_deltaT>dt_3mth, F.lag("job_begin_last", 1).over(w_deltaT)+dt_3mth).otherwise(-1)
    #f_del_date_2mth = F.when( f_deltaT>dt_2mth, F.lag("job_begin_last", 1).over(w_deltaT)+dt_2mth).otherwise(-1)
    #f_del_date_1mth = F.when( f_deltaT>dt_1mth, F.lag("job_begin_last", 1).over(w_deltaT)+dt_1mth).otherwise(-1)
    
    # Check if last access is > time window
    jm_todate = jm_date(todate)
    to_unix   = jm_date_unix(jm_todate)
        
    f_check_last_6mth = F.when( ((F.col("last_job_begin")==F.last("last_job_begin").over(w_deltaT))&(to_unix-F.col("last_job_begin")>dt_6mth)), F.last("pbr_size").over(w_deltaT) ).otherwise(0.0)
    f_check_last_5mth = F.when( ((F.col("last_job_begin")==F.last("last_job_begin").over(w_deltaT))&(to_unix-F.col("last_job_begin")>dt_5mth)), F.last("pbr_size").over(w_deltaT) ).otherwise(0.0)
    f_check_last_4mth = F.when( ((F.col("last_job_begin")==F.last("last_job_begin").over(w_deltaT))&(to_unix-F.col("last_job_begin")>dt_4mth)), F.last("pbr_size").over(w_deltaT) ).otherwise(0.0)
    f_check_last_3mth = F.when( ((F.col("last_job_begin")==F.last("last_job_begin").over(w_deltaT))&(to_unix-F.col("last_job_begin")>dt_3mth)), F.last("pbr_size").over(w_deltaT) ).otherwise(0.0)
    f_check_last_2mth = F.when( ((F.col("last_job_begin")==F.last("last_job_begin").over(w_deltaT))&(to_unix-F.col("last_job_begin")>dt_2mth)), F.last("pbr_size").over(w_deltaT) ).otherwise(0.0)
    f_check_last_1mth = F.when( ((F.col("last_job_begin")==F.last("last_job_begin").over(w_deltaT))&(to_unix-F.col("last_job_begin")>dt_1mth)), F.last("pbr_size").over(w_deltaT) ).otherwise(0.0)
    
    deltaT_df = join_df.withColumn("delta_t", f_deltaT)\
                       .withColumn("check_dt_6mth", f_check_dt_6mth)\
                       .withColumn("check_dt_5mth", f_check_dt_5mth)\
                       .withColumn("check_dt_4mth", f_check_dt_4mth)\
                       .withColumn("check_dt_3mth", f_check_dt_3mth)\
                       .withColumn("check_dt_2mth", f_check_dt_2mth)\
                       .withColumn("check_dt_1mth", f_check_dt_1mth)\
                       .withColumn("check_last_6mth", f_check_last_6mth)\
                       .withColumn("check_last_5mth", f_check_last_5mth)\
                       .withColumn("check_last_4mth", f_check_last_4mth)\
                       .withColumn("check_last_3mth", f_check_last_3mth)\
                       .withColumn("check_last_2mth", f_check_last_2mth)\
                       .withColumn("check_last_1mth", f_check_last_1mth)\
                       .fillna(0.0,"delta_t")\
                       .fillna(0.0,["check_dt_6mth","check_dt_5mth","check_dt_4mth","check_dt_3mth","check_dt_2mth","check_dt_1mth"])\
                       .fillna(0.0,["check_last_6mth","check_last_5mth","check_last_4mth","check_last_3mth","check_last_2mth","check_last_1mth"])

    print_rows(deltaT_df, "deltaT_df", verbose)
            
    # perform aggregation
    print("### Aggregating final datframe")
    join_agg = deltaT_df.groupBy(['site','dataset'])\
                        .agg( F.sum('nJobs').alias('nJobsTot'),
                              F.last('pbr_size').alias('size'),
                              F.max('delta_t').alias('max_deltaT'),
                              F.sum('check_dt_6mth').alias('total_recalled_6mth'),
                              F.sum('check_dt_5mth').alias('total_recalled_5mth'),
                              F.sum('check_dt_4mth').alias('total_recalled_4mth'),
                              F.sum('check_dt_3mth').alias('total_recalled_3mth'),
                              F.sum('check_dt_2mth').alias('total_recalled_2mth'),
                              F.sum('check_dt_1mth').alias('total_recalled_1mth'),
                              F.sum(F.col('check_last_6mth')+F.col('check_dt_6mth')).alias('total_deleted_6mth'),
                              F.sum(F.col('check_last_5mth')+F.col('check_dt_5mth')).alias('total_deleted_5mth'),
                              F.sum(F.col('check_last_4mth')+F.col('check_dt_4mth')).alias('total_deleted_4mth'),
                              F.sum(F.col('check_last_3mth')+F.col('check_dt_3mth')).alias('total_deleted_3mth'),
                              F.sum(F.col('check_last_2mth')+F.col('check_dt_2mth')).alias('total_deleted_2mth'),
                              F.sum(F.col('check_last_1mth')+F.col('check_dt_1mth')).alias('total_deleted_1mth')  )
    
    print_rows(join_agg, "join_agg", verbose)
    
                                     
    # write out results back to HDFS, the fout parameter defines area on HDFS
    # it is either absolute path or area under /user/USERNAME
    if  fout:
        print("### Writing out final table")
        ndf = split_dataset(join_agg, 'dataset')
        ndf.persist(StorageLevel.MEMORY_AND_DISK)
        fout_temp = "%s/analyze_agg_%s_%s/" % (fout,fromdate,todate)
        ndf.write.format("com.databricks.spark.csv")\
                 .option("header", "true").save(fout_temp)

        
    # stop sparky
    #ctx.stop()
    

def run(fromdate, todate, maxtime, fout, yarn, verbose, inst):

    ctx = spark_context('cms', yarn, verbose)
    sqlContext = HiveContext(ctx)

    run_agg_sources(ctx,sqlContext,fromdate, todate, maxtime, fout, yarn, verbose, inst)
    run_analyze_agg(ctx,sqlContext,fromdate, todate, maxtime, fout, yarn, verbose, inst)

    ctx.stop()
    

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()

    print("Input arguments: %s" % opts)
    time0 = time.time()
    inst = opts.inst

    if  inst in ['global', 'phys01', 'phys02', 'phys03']:
        inst = inst.upper()
    else:
        raise Exception('Unsupported DBS instance "%s"' % inst)

    run(opts.fromdate, opts.todate, opts.maxtime, opts.fout, opts.yarn, opts.verbose, inst)
    #run_agg_sources(opts.fromdate, opts.todate, opts.maxtime, opts.fout, opts.yarn, opts.verbose, inst)
    #run_analyze_agg(opts.fromdate, opts.todate, opts.maxtime, opts.fout, opts.yarn, opts.verbose, inst)

    print('Start time  : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time0)))
    print('End time    : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time.time())))
    print('Elapsed time: %s sec' % elapsed_time(time0))

if __name__ == '__main__':
    main()
