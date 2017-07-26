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
from CMSSpark.spark_utils import spark_context, aaa_tables, split_dataset, split_dataset_noDrop
from CMSSpark.utils import elapsed_time


# Globals, because they wont ever change
dt_6mth = 60*60*24*30*6
dt_5mth = 60*60*24*30*5
dt_4mth = 60*60*24*30*4
dt_3mth = 60*60*24*30*3
dt_2mth = 60*60*24*30*2
dt_1mth = 60*60*24*30*1
    
interval= 60*60*24*20


# Classes
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
        self.parser.add_argument("--run_all", action="store_true",
            dest="run_all", default=False, help='Command to run aggregation of phedex, jobMonitoring, and analyze the results')
        self.parser.add_argument("--run_agg_ph", action="store_true",
            dest="run_agg_ph", default=False, help='Command to run aggregation of phedex')
        self.parser.add_argument("--run_agg_jm", action="store_true",
            dest="run_agg_jm", default=False, help='Command to run aggregation of jobMonitoring')
        self.parser.add_argument("--run_analyze", action="store_true",
            dest="run_analyze", default=False, help='Command to run analysis of aggregated results')
        self.parser.add_argument("--fout", action="store",
            dest="fout", default=fout, help='Output file name, default %s' % fout)
        self.parser.add_argument("--fromdate", action="store",
            dest="fromdate", default="", help='Select CMSSW data for date to begin (YYYYMMDD)')
        self.parser.add_argument("--todate", action="store",
            dest="todate", default="", help='Select CMSSW data for date to end (YYYYMMDD)')
        self.parser.add_argument("--agg_interval", action="store",
            dest="agg_interval", default=interval, help='Select maximum time window for aggregating phedex and jobMontioring in days')
        self.parser.add_argument("--agg_by_site", action="store", type=string2bool, nargs='?', 
            dest="agg_by_site", default=True, help='Aggregate Phedex/JobMonitoring information by dataset and site')
        msg= 'DBS instance on HDFS: global (default), phys01, phys02, phys03'
        self.parser.add_argument("--inst", action="store",
            dest="inst", default="global", help=msg)
        self.parser.add_argument("--no-log4j", action="store_true",
            dest="no-log4j", default=False, help="Disable spark log4j messages")
        self.parser.add_argument("--yarn", action="store_true",
            dest="yarn", default=False, help="run job on analytics cluster via yarn resource manager")
        self.parser.add_argument("--verbose", action="store_true",
            dest="verbose", default=False, help="verbose output")



# Helper Functions
def string2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


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



# Schemas
def schema_agg_phedex():
    """
    DBS DATASET_AGG_SOURCES table schema
    :returns: StructType consisting StructField array
    """
    return StructType([
             StructField("ph_interval", IntegerType(), True),
             StructField("node_name", StringType(), True),
             StructField("dataset_name", StringType(), True),
             StructField("dataset_is_open", StringType(), True),
             StructField("avg_pbr_size", DoubleType(), True)
             ])

def schema_agg_jm(agg_by_site=True):
    """
    DBS DATASET_AGG_SOURCES table schema
    :returns: StructType consisting StructField array
    """
    if agg_by_site:
        return StructType([
                StructField("d_dataset", StringType(), True),
                StructField("d_nevents", IntegerType(), True),
                StructField("d_size", DoubleType(), True),
                StructField("d_creation_date", DoubleType(), True),
                StructField("job_interval", IntegerType(), True),
                StructField("job_site", StringType(), True),
                StructField("nJobs", IntegerType(), True),
                StructField("first_job_begin", DoubleType(), True),
                StructField("last_job_begin", DoubleType(), True),
                StructField("tot_wc", DoubleType(), True),
                StructField("tot_cpu", DoubleType(), True),
                StructField("interval_max_delta_t", DoubleType(), True)
                ])
    else:
        return StructType([
                StructField("d_dataset", StringType(), True),
                StructField("d_nevents", IntegerType(), True),
                StructField("d_size", DoubleType(), True),
                StructField("d_creation_date", DoubleType(), True),
                StructField("job_interval", IntegerType(), True),
                StructField("nJobs", IntegerType(), True),
                StructField("first_job_begin", DoubleType(), True),
                StructField("last_job_begin", DoubleType(), True),
                StructField("tot_wc", DoubleType(), True),
                StructField("tot_cpu", DoubleType(), True),
                StructField("interval_max_delta_t", DoubleType(), True)
                ])

    
# Aggregate Phedex Records
def run_agg_phedex(ctx,sqlContext, start_date, end_date, agg_interval, fout, yarn=None, verbose=None, inst='GLOBAL'):
    """
    Main function to run pyspark job. It requires a schema file, an HDFS directory
    with data and optional script with mapper/reducer functions.
    """
    
    # input sanitation, check time delta between fromdate and todate
    if len(start_date) != 8:
        raise Exception("Given start_date %s is not in YYYYMMDD format" %s)
    if len(end_date) != 8:
        raise Exception("Given end_date %s is not in YYYYMMDD format" %s)
    
    # Dictionary for tables
    tables = {}
    
    # read Phedex tables, has info about where datasets are stored, and size of blocks
    ph_fromdate = "%s-%s-%s" % (start_date[:4], start_date[4:6], start_date[-2:])
    ph_todate   = "%s-%s-%s" % (end_date[:4],   end_date[4:6],   end_date[-2:])
    tables.update(phedex_tables_multiday(sqlContext, verbose=verbose, fromdate=ph_fromdate, todate=ph_todate))
    ph_df       = tables['phedex_df']
       
    # add sortable date columns to phedex records
    ph_df = ph_df.withColumn("now_day",     F.date_format(ph_df["now_sec"].cast('timestamp'),"yyyyMMdd"))\
                 .withColumn("ph_interval", F.floor(ph_df["now_sec"]/agg_interval) )
    print_rows(ph_df, "ph_df", verbose)
    
    # aggregate phedex information, to get dataset size for each site, for each day
    ph_sel_cols  = ['ph_interval', 'now_day', 'node_name', 'dataset_name', 'dataset_is_open', 'block_bytes']
    ph_gb_cols   = ['ph_interval', 'now_day', 'node_name', 'dataset_name', 'dataset_is_open']
    ph_df_agg_v0 = ph_df.select(ph_sel_cols)\
                        .groupBy(ph_gb_cols)\
                        .agg( F.sum('block_bytes').alias('pbr_size') )
    print_rows(ph_df_agg_v0, "ph_df_agg_v0", verbose)
    
    # Aggregate accross interval, getting avg size for each dataset
    ph_sel_cols  = ['ph_interval', 'node_name', 'dataset_name', 'dataset_is_open', 'pbr_size']
    ph_gb_cols   = ['ph_interval', 'node_name', 'dataset_name', 'dataset_is_open']
    ph_df_agg_vf = ph_df_agg_v0.select(ph_sel_cols)\
                               .groupBy(ph_gb_cols)\
                               .agg( F.avg('pbr_size').alias('avg_pbr_size') )
    print_rows(ph_df_agg_vf, "ph_df_agg_vf", verbose)

    # write out results back to HDFS, the fout parameter defines area on HDFS
    # it is either absolute path or area under /user/USERNAME
    if  fout:
        print("### Writing out source aggregation table for %s to %s" % (start_date, end_date) )
        fout_temp = "%s/agg_phedex_%s_%s/" % (fout,start_date,end_date)
        ndf = split_dataset_noDrop(ph_df_agg_vf, 'dataset_name')
        ndf.persist(StorageLevel.MEMORY_AND_DISK)
        ndf.write.format("com.databricks.spark.csv")\
                 .option("header", "true").save(fout_temp)
        ndf.unpersist()
    


# Aggregate job monitoring records
def run_agg_jm(ctx,sqlContext,start_date, end_date, agg_interval, fout, agg_by_site=True, yarn=None, verbose=None, inst='GLOBAL'):
    """
    Main function to run pyspark job. It requires a schema file, an HDFS directory
    with data and optional script with mapper/reducer functions.
    """
    
    # input sanitation, check time delta between fromdate and todate
    if len(start_date) != 8:
        raise Exception("Given start_date %s is not in YYYYMMDD format" %s)
    if len(end_date) != 8:
        raise Exception("Given end_date %s is not in YYYYMMDD format" %s)
    
    # read DBS tables, has info about datasets
    tables = {}
    tables.update(dbs_tables(sqlContext, inst=inst, verbose=verbose))
    dbs_d_df = tables['ddf'] # dataset table
    dbs_f_df = tables['fdf'] # file table
    
    dbs_sel_cols = ['d_dataset_id', 'd_dataset', 'd_creation_date', 'f_event_count','f_file_size']
    stmt  = 'SELECT %s FROM ddf ' % ','.join(dbs_sel_cols) 
    stmt += 'JOIN fdf on ddf.d_dataset_id = fdf.f_dataset_id '
    dbs_df = sqlContext.sql(stmt)
    
    dbs_gb_cols = ['d_dataset_id', 'd_dataset']
    dbs_agg = dbs_df.groupBy(dbs_gb_cols)\
                    .agg( F.sum('f_event_count').alias('d_evts'),
                          F.sum('f_file_size').alias('d_size'),
                          F.max('d_creation_date').alias('d_creation_date') )
    dbs_agg.registerTempTable('dbs_agg')
    print_rows(dbs_agg, "dbs_agg", verbose)

 
    # read job monitoring, avro rdd file for a given date
    jm_fromdate = jm_date(start_date)
    jm_todate   = jm_date(end_date)
    tables.update(jm_tables_multiday(ctx, sqlContext, fromdate=jm_fromdate, todate=jm_todate, verbose=verbose))
    jm_df       = tables['jm_df']
        
    # add sortable column to job monitoring records, note jobMonitoring timeStamp in miliseconds, convert to seconds
    f_job_day      = F.date_format((F.col("StartedRunningTimeStamp")*0.001).cast('timestamp'),"yyyyMMdd")
    f_job_sec      = (F.col("StartedRunningTimeStamp")*0.001)
    f_job_interval = F.floor(((F.col("StartedRunningTimeStamp")*0.001)/agg_interval))
    jm_df_date = jm_df.withColumn("job_begin_sec",  f_job_sec )\
                      .withColumn("jm_interval",    f_job_interval )\
                      .withColumn("job_day",        f_job_day )
    jm_df_date.registerTempTable('jm_df_date')
    print_rows(jm_df_date, "jm_df_date", verbose)
    
    
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
    cols = ['dbs_agg.d_dataset AS d_dataset',
            'dbs_agg.d_evts AS d_nevents',
            'dbs_agg.d_size AS d_size',
            'dbs_agg.d_creation_date AS d_creation_date',
            'jm_df_date.SiteName AS job_site',
            'jm_df_date.FileName AS job_fileName',
            'jm_df_date.JobId AS job_id',
            'jm_df_date.job_begin_sec AS job_begin',
            'jm_df_date.jm_interval AS job_interval',
            'jm_df_date.WrapWC AS job_wrap_wc',
            'jm_df_date.WrapCPU AS job_wrap_cpu'
            ]
    stmt  = 'SELECT DISTINCT %s FROM jm_df_date ' % ','.join(cols)       # Select distinct, found duplicate jobs
    stmt += 'JOIN fdf ON fdf.f_logical_file_name = jm_df_date.FileName ' # Join jobMonitoring to DBS file dataframes based on fileName
    stmt += 'JOIN dbs_agg ON dbs_agg.d_dataset_id = fdf.f_dataset_id '   # Join DBS file and dataset dataframes based on datasetID
    stmt += 'WHERE jm_df_date.Type = "analysis" '                        # Select on analysis type jobs
    join_df = sqlContext.sql(stmt)
    print_rows(join_df, stmt, verbose)

    # Use Window function to calculate Delta T between accesses on dataset
    if agg_by_site: agg_cols = ['d_dataset','job_site']
    else:           agg_cols = ['d_dataset']
    w_dt  = Window.partitionBy(agg_cols).orderBy("job_begin")
    f_dt  = F.col("job_begin")-F.lag("job_begin",1).over(w_dt)
    dt_df = join_df.withColumn("job_delta_t", f_dt).fillna(0.0,"job_delta_t")

    # aggregate dataframe
    if agg_by_site: join_agg_cols = ['d_dataset', 'd_nevents', 'd_size', 'd_creation_date', 'job_interval', 'job_site']
    else:           join_agg_cols = ['d_dataset', 'd_nevents', 'd_size', 'd_creation_date', 'job_interval']
    join_df_agg = dt_df.groupBy(join_agg_cols)\
                       .agg( F.count('job_fileName').alias('nJobs'),
                             F.min('job_begin').alias('first_job_begin'),
                             F.max('job_begin').alias('last_job_begin'),
                             F.sum('job_wrap_wc').alias('tot_wc'),
                             F.sum('job_wrap_cpu').alias('tot_cpu'),
                             F.max('job_delta_t').alias('interval_max_delta_t') )
    print_rows(join_df_agg, "join_df_agg", verbose)
        
    # write out results back to HDFS, the fout parameter defines area on HDFS
    # it is either absolute path or area under /user/USERNAME
    if  fout:
        print("### Writing out source aggregation table for %s to %s" % (start_date, end_date) )
        fout_temp = "%s/agg_jm_%s_%s/" % (fout,start_date,end_date)
        ndf = split_dataset_noDrop(join_df_agg, 'd_dataset')
        ndf.persist(StorageLevel.MEMORY_AND_DISK)
        ndf.write.format("com.databricks.spark.csv")\
                 .option("header", "true").save(fout_temp)
        ndf.unpersist()
    


# Analyze Aggregated Records            
def run_analyze_agg(ctx,sqlContext,fromdate, todate, fout, agg_by_site=True, yarn=None, verbose=None, inst='GLOBAL'):        
    """
    Main function to run pyspark job. It requires a schema file, an HDFS directory
    with data and optional script with mapper/reducer functions.
    """

    # input sanitation, check time delta between fromdate and todate
    if len(fromdate) != 8:
        raise Exception("Given fromdate %s is not in YYYYMMDD format" %s)
    if len(todate) != 8:
        raise Exception("Given todate %s is not in YYYYMMDD format" %s)
    
    # Read aggregated phedex data
    f_input = []
    temp_path = fout
    if fout[-1]!="/":
        temp_path += "/"
    f_input.append("%sagg_phedex_%s_%s/part*" % (temp_path,fromdate,todate))
    
    ph_df = unionAll([sqlContext.read.format('com.databricks.spark.csv')
                        .options(header='true',treatEmptyValuesAsNulls='true', nullValue='null')\
                        .load(path, schema = schema_agg_phedex()) \
                        for path in f_input])
    ph_df.registerTempTable('ph_df')

    if not agg_by_site: 
        ph_df_agg = ph_df.groupby(["ph_interval","dataset_name","dataset_is_open","avg_pbr_size"])\
                         .agg( F.count("node_name").alias("nSites"),
                               F.sum("avg_pbr_size").alias("avg_pbr_size_allsites") )
        ph_df_agg.registerTempTable('ph_df_agg')                         
                  

    # Read aggregated jm data 
    f_input = []
    temp_path = fout
    if fout[-1]!="/":
        temp_path += "/"
    f_input.append("%sagg_jm_%s_%s/part*" % (temp_path,fromdate,todate))
    
    jm_df = unionAll([sqlContext.read.format('com.databricks.spark.csv')
                        .options(header='true',treatEmptyValuesAsNulls='true', nullValue='null')\
                        .load(path, schema = schema_agg_jm(agg_by_site)) \
                        for path in f_input])
    jm_df.registerTempTable('jm_df')
    #print("NJobs in JobMonitoring: \n")
    #print(jm_df.agg(F.sum("nJobs")).show())

    # Columns for JOINS
    if agg_by_site:
        #'jm_df.d_nevents AS d_nevents',
        #'jm_df.d_size AS d_size',
        #'jm_df.d_creation_date AS d_creation_date',
        cols = ['ph_df.dataset_name AS dataset',
                'ph_df.node_name AS site',
                'ph_df.avg_pbr_size AS pbr_size',
                'jm_df.nJobs AS nJobs',
                'jm_df.first_job_begin AS first_job_begin',
                'jm_df.last_job_begin AS last_job_begin',
                'jm_df.tot_wc AS tot_wc',
                'jm_df.tot_cpu AS tot_cpu',
                'jm_df.interval_max_delta_t AS interval_max_delta_t'
                ]
    else:
        #'jm_df.d_nevents AS d_nevents',
        #'jm_df.d_size AS d_size',
        #'jm_df.d_creation_date AS d_creation_date',
        cols = ['ph_df_agg.dataset_name AS dataset',
                'ph_df_agg.nSites AS nSites',
                'ph_df_agg.avg_pbr_size_allsites AS pbr_size_allsites',
                'jm_df.nJobs AS nJobs',
                'jm_df.first_job_begin AS first_job_begin',
                'jm_df.last_job_begin AS last_job_begin',
                'jm_df.tot_wc AS tot_wc',
                'jm_df.tot_cpu AS tot_cpu',
                'jm_df.interval_max_delta_t AS interval_max_delta_t'
                ]
        
    stmt  = 'SELECT %s FROM jm_df ' % ','.join(cols) # Select distinct, found duplicate jobs
    if agg_by_site: stmt += 'LEFT JOIN ph_df ON (ph_df.node_name=jm_df.job_site AND ph_df.dataset_name=jm_df.d_dataset AND ph_df.ph_interval=jm_df.job_interval) ' 
    else:           stmt += 'JOIN ph_df_agg ON (ph_df_agg.dataset_name=jm_df.d_dataset AND ph_df_agg.ph_interval=jm_df.job_interval) ' 
    join_df = sqlContext.sql(stmt)
    print_rows(join_df, stmt, verbose)
    #print("NJobs in JobMonitoring After Joining with Phedex: \n")
    #print(join_df.agg(F.sum("nJobs")).show())

    ## ARRRRGGGHHH!!!!! QUIT HERE, WHY AM I LOSING SO MANY JOBS WHEN JOINING JM AND PHEDEX?!!!
    #null_df = join_df.where(F.col("dataset").isNotNull()).count()
    #print( "NULL DF has %s entries" % null_df)
    #print_rows(null_df, "NULL DF", verbose=True)

    # Use Window function to calculate Delta T between accesses on dataset
    if agg_by_site: agg_cols = ['dataset','site']
    else:           agg_cols = ['dataset']
    w_dt = Window.partitionBy(agg_cols).orderBy("first_job_begin")

    # Functions for deltaT calculation
    f_dt_btw_interval = F.col("first_job_begin")-F.lag("last_job_begin",1).over(w_dt)
    f_dt_win_interval = F.col("interval_max_delta_t")
    f_deltaT          = F.when( f_dt_btw_interval > f_dt_win_interval, f_dt_btw_interval).otherwise(f_dt_win_interval)
    
    if agg_by_site: f_size = F.col("pbr_size")
    else          : f_size = F.col("pbr_size_allsites")

    # Check if Delta_t is greater than some time window
    f_check_dt_6mth = F.when( f_deltaT>dt_6mth, f_size ).otherwise(0.0)
    f_check_dt_5mth = F.when( f_deltaT>dt_5mth, f_size ).otherwise(0.0)
    f_check_dt_4mth = F.when( f_deltaT>dt_4mth, f_size ).otherwise(0.0)
    f_check_dt_3mth = F.when( f_deltaT>dt_3mth, f_size ).otherwise(0.0)
    f_check_dt_2mth = F.when( f_deltaT>dt_2mth, f_size ).otherwise(0.0)
    f_check_dt_1mth = F.when( f_deltaT>dt_1mth, f_size ).otherwise(0.0)
    
    # Calc deletion date for different time frames
    #f_last_access = F.lag("job_begin_last", 1).over(w_dt)
    #f_del_date_6mth = F.when( f_deltaT>dt_6mth, f_last_access+dt_6mth).otherwise(-1)
    #f_del_date_5mth = F.when( f_deltaT>dt_5mth, f_last_access+dt_5mth).otherwise(-1)
    #f_del_date_4mth = F.when( f_deltaT>dt_4mth, f_last_access+dt_4mth).otherwise(-1)
    #f_del_date_3mth = F.when( f_deltaT>dt_3mth, f_last_access+dt_3mth).otherwise(-1)
    #f_del_date_2mth = F.when( f_deltaT>dt_2mth, f_last_access+dt_2mth).otherwise(-1)
    #f_del_date_1mth = F.when( f_deltaT>dt_1mth, f_last_access+dt_1mth).otherwise(-1)
    
    # Check if last access is > time window
    jm_todate         = jm_date(todate)
    to_unix           = jm_date_unix(jm_todate)
    f_isLastJob       = (F.col("last_job_begin")==F.last("last_job_begin").over(w_dt))
    f_dt_lastJob_now  = (to_unix-F.col("last_job_begin"))
    if agg_by_site: f_last_size = F.last("pbr_size").over(w_dt)
    else:           f_last_size = F.last("pbr_size_allsites").over(w_dt)
    f_check_last_6mth = F.when( ((f_isLastJob) & (f_dt_lastJob_now>dt_6mth)), f_last_size ).otherwise(0.0)
    f_check_last_5mth = F.when( ((f_isLastJob) & (f_dt_lastJob_now>dt_5mth)), f_last_size ).otherwise(0.0)
    f_check_last_4mth = F.when( ((f_isLastJob) & (f_dt_lastJob_now>dt_4mth)), f_last_size ).otherwise(0.0)
    f_check_last_3mth = F.when( ((f_isLastJob) & (f_dt_lastJob_now>dt_3mth)), f_last_size ).otherwise(0.0)
    f_check_last_2mth = F.when( ((f_isLastJob) & (f_dt_lastJob_now>dt_2mth)), f_last_size ).otherwise(0.0)
    f_check_last_1mth = F.when( ((f_isLastJob) & (f_dt_lastJob_now>dt_1mth)), f_last_size ).otherwise(0.0)
    
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
    print("### Aggregating final dataframe")
    if agg_by_site:
        #join_agg = deltaT_df.groupBy(['site','dataset','d_nevents','d_size','d_creation_date'])\
        join_agg = deltaT_df.groupBy(['site','dataset'])\
                            .agg( F.sum('nJobs').alias('nJobsTot'),
                                  F.sum('tot_wc').alias('wrap_wc_tot'),
                                  F.sum('tot_cpu').alias('wrap_cpu_tot'),
                                  F.first('first_job_begin').alias('firstAccess'),
                                  F.first('last_job_begin').alias('lastAccess'),
                                  F.avg('pbr_size').alias('avg_pbr_size'),
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

    else:
        #join_agg = deltaT_df.groupBy(['dataset','d_nevents','d_size','d_creation_date'])\
        join_agg = deltaT_df.groupBy(['dataset'])\
                            .agg( F.sum('nJobs').alias('nJobsTot'),
                                  F.avg('nSites').alias('avg_site_occ'),
                                  F.min('nSites').alias('min_site_occ'),
                                  F.max('nSites').alias('max_site_occ'),
                                  F.sum('tot_wc').alias('wrap_wc_tot'),
                                  F.sum('tot_cpu').alias('wrap_cpu_tot'),
                                  F.first('first_job_begin').alias('firstAccess'),
                                  F.first('last_job_begin').alias('lastAccess'),
                                  F.avg('pbr_size_allsites').alias('avg_pbr_size_allsites'),
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
        ndf = split_dataset_noDrop(join_agg, 'dataset')
        ndf.persist(StorageLevel.MEMORY_AND_DISK)
        fout_temp = "%s/analyze_agg_%s_%s/" % (fout,fromdate,todate)
        ndf.write.format("com.databricks.spark.csv")\
                 .option("header", "true").save(fout_temp)
        ndf.unpersist()

        
### MAIN ####
def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()

    # Get input arguments
    print("Input arguments: %s" % opts)
    time0 = time.time()
    inst = opts.inst

    # Parse which DBS instance to use
    if  inst in ['global', 'phys01', 'phys02', 'phys03']:
        inst = inst.upper()
    else:
        raise Exception('Unsupported DBS instance "%s"' % inst)

    # Parse which run option to use
    if not opts.run_all and not opts.run_agg_ph and not opts.run_agg_jm and not opts.run_analyze:
        raise Exception('Must set either: --run_all, --run_agg_ph, --run_agg_jm, or --run_analyze, see --help for details')

    # Begin Spark Context
    ctx = spark_context('cms', opts.yarn, opts.verbose)
    sqlContext = HiveContext(ctx)
    
    # Run phedex aggregation
    if opts.run_all or opts.run_agg_ph:
        run_agg_phedex(ctx,sqlContext,opts.fromdate, opts.todate, opts.agg_interval, opts.fout, opts.yarn, opts.verbose, inst)

    if opts.run_all or opts.run_agg_jm:
        run_agg_jm(ctx,sqlContext,opts.fromdate, opts.todate, opts.agg_interval, opts.fout, opts.agg_by_site, opts.yarn, opts.verbose, inst)

    if opts.run_all or opts.run_analyze:
        run_analyze_agg(ctx,sqlContext,opts.fromdate, opts.todate, opts.fout, opts.agg_by_site, opts.yarn, opts.verbose, inst)

    # End Spark Context
    ctx.stop()

    #run(opts.fromdate, opts.todate, opts.agg_interval, opts.fout, opts.yarn, opts.verbose, inst)
    
    print('Start time  : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time0)))
    print('End time    : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time.time())))
    print('Elapsed time: %s sec' % elapsed_time(time0))

if __name__ == '__main__':
    main()
