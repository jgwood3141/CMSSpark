#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : cmsSpark_storageStudies_stats.py
Author     : John Wood jgwood@ucsd.physics.edu
Description: 
"""

# system modules
import os
import sys
import argparse
import datetime as dt

import pandas as pd
import numpy as np

import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from   matplotlib.backends.backend_pdf import PdfPages
from   matplotlib.gridspec import GridSpec

interval = 60*60*24*20
pbytes   = np.power(1024, 5)

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--din", action="store",
            dest="din", default="", help="Input file")
        self.parser.add_argument("--dout", action="store",
            dest="dout", default="./", help="Output directory for plots")
        self.parser.add_argument("--agg_by_site", action="store", type=string2bool, nargs='?',
            dest="agg_by_site", default=True, help='Aggregate Phedex/JobMonitoring information by dataset and site')
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

def summary_jm_aggBySite(din, dout):

    # Grab Job Monitoring CSV
    df = pd.read_csv("%s/jm_agg.csv" % din)

    # Pre-selection, basic filtering
    df = df[ df["job_interval"]>0.0 ]
    df["job_interval_sec"]  = df["job_interval"]*interval
    
    # Basic info about datafram
    print("\n### DataFrame Information ###\n")
    first_job_date = dt.datetime.fromtimestamp( df["first_job_begin"].min() ).strftime("%Y-%m-%d")
    last_job_date  = dt.datetime.fromtimestamp( df["last_job_begin"].max() ).strftime("%Y-%m-%d")
    print("    Job Time Range: %s to %s \n" % (first_job_date,last_job_date))
    tot_jobs = df["nJobs"].sum()
    print("    nJobs total:    %s \n" % tot_jobs )
    h_intervals = np.unique(mdates.epoch2num(df.job_interval_sec))
    
    # Site Info
    print("\n### Site Information ###\n")
    sites = np.unique(df.job_site)
    print("   %s unique sites\n" % len(sites))
    print( sites )
    df_agg = df.groupby("job_site").agg({"nJobs":sum}).sort_values("nJobs", ascending=False).reset_index()
    df_agg["percent"] = 100.0*df_agg["nJobs"]/tot_jobs
    print("\n   Ranked Sites by Number of Jobs \n")
    print( df_agg )

    # Group data by T1, T2, T3
    df_t1 = df[df.job_site.str.contains('T1_')]
    df_t2 = df[df.job_site.str.contains('T2_')]
    df_t3 = df[df.job_site.str.contains('T3_')]
    df_by_tier = [ df_t1, df_t2, df_t3 ]

    tot_jobs_t1 = df_t1["nJobs"].sum()
    tot_jobs_t2 = df_t2["nJobs"].sum()
    tot_jobs_t3 = df_t3["nJobs"].sum()
    print("\n  Number of Jobs by Computing Tier \n")
    print("    nJobs at T1 sites: %s" % tot_jobs_t1 )
    print("    nJobs at T2 sites: %s" % tot_jobs_t2 )
    print("    nJobs at T3 sites: %s" % tot_jobs_t3 )
  
    # nJobs vs interval, T-X sites
    with PdfPages('%s/siteUsage_vs_interval__TXsites.pdf' % dout) as pdf:
        fig, ax = plt.subplots()
        labels, x_vals, y_vals = [], [], []
        for idx, idf in enumerate(df_by_tier):
            labels.append( "T%s Sites" % (idx+1) )
            x_vals.append( mdates.epoch2num(idf["job_interval_sec"]) )
            y_vals.append( idf["nJobs"] )
        plt.hist( x=x_vals, weights=y_vals, bins=h_intervals, label=labels, stacked=True )
        fig.autofmt_xdate()
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        plt.legend(loc="upper left")
        plt.xlabel("Date")
        plt.ylabel("Number of Jobs")
        plt.title("Number of Jobs vs Time, T-X sites")
        plt.grid(True)
        pdf.savefig()
        plt.close()

    # nJobs vs interval, top X sites 
    topX_sites = 5
    top_sites = df_agg["job_site"].head(topX_sites)
    with PdfPages('%s/siteUsage_vs_interval__Top%ssites.pdf' % (dout,topX_sites)) as pdf:
        fig, ax = plt.subplots()
        labels, x_vals, y_vals = [], [], []
        for site in top_sites:
            idf = df[df.job_site==site]
            labels.append( "%s" % site )
            x_vals.append( mdates.epoch2num(idf["job_interval_sec"]) )
            y_vals.append( idf["nJobs"] )
        plt.hist( x=x_vals, weights=y_vals, bins=h_intervals, label=labels, stacked=True )
        fig.autofmt_xdate()
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        plt.legend(loc="upper left")
        plt.xlabel("Date")
        plt.ylabel("Number of Jobs")
        plt.title("Number of Jobs vs Time, Top %s sites" % topX_sites )
        plt.grid(True)
        pdf.savefig()
        plt.close()

    # Dataset Tier Info
    print("\n### Dataset Information By Tier ###\n")
    df_agg = df.groupby("tier").agg({"nJobs":sum}).sort_values("nJobs", ascending=False).reset_index()
    df_agg["percent"] = 100.0*df_agg["nJobs"]/tot_jobs
    print("\n   Ranked Dataset Tiers by Number of Jobs \n")
    print( df_agg )
    tiers = df_agg["tier"]
    
    # nJobs vs interval, by tier
    with PdfPages('%s/siteUsage_vs_interval__tier.pdf' % dout) as pdf:
        fig, ax = plt.subplots()
        labels, x_vals, y_vals = [], [], []
        for tier in tiers:
            idf = df[df.tier==tier]
            labels.append( "%s" % tier )
            x_vals.append( mdates.epoch2num(idf["job_interval_sec"]) )
            y_vals.append( idf["nJobs"] )
        plt.hist( x=x_vals, weights=y_vals, bins=h_intervals, label=labels, stacked=True )
        fig.autofmt_xdate()
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        plt.legend(loc="upper left", fontsize="xx-small", ncol=2)
        plt.xlabel("Date")
        plt.ylabel("Number of Jobs")
        plt.title("Number of Jobs vs Time, By Dataset Tier"  )
        plt.grid(True)
        pdf.savefig()
        plt.close()

    # Primary Datasets Information
    topX_primds = 10
    primds = np.unique(df.primds)
    print("\n   %s unique primary datasets\n" % len(primds))
    df_agg = df.groupby("primds").agg({"nJobs":sum}).sort_values("nJobs", ascending=False).reset_index()
    df_agg["percent"] = 100.0*df_agg["nJobs"]/tot_jobs
    print("\n   Ranked Datasets by Number of Jobs \n")
    top_primds = df_agg["primds"].head(topX_primds)
    print( df_agg.head(topX_primds*3) )
   
    # nJobs vs interval, by primary dataset
    with PdfPages('%s/siteUsage_vs_interval__primds.pdf' % dout) as pdf:
        fig, ax = plt.subplots()
        labels, x_vals, y_vals = [], [], []
        for primds in top_primds:
            idf = df[df.primds==primds]
            labels.append( "%s" % primds )
            x_vals.append( mdates.epoch2num(idf["job_interval_sec"]) )
            y_vals.append( idf["nJobs"] )
        plt.hist( x=x_vals, weights=y_vals, bins=h_intervals, label=labels, stacked=True )
        fig.autofmt_xdate()
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        plt.legend(loc="upper left", fontsize="xx-small")
        plt.xlabel("Date")
        plt.ylabel("Number of Jobs")
        plt.title("Number of Jobs vs Time, By Top %s Primary Datasets"% topX_primds  )
        plt.grid(True)
        pdf.savefig()
        plt.close()


def summary_analyze_agg_aggBySite(din, dout):
    
    print("\n\n\n\n")
    print("Analyzing Aggregation of Phedex and Job Monitoring")
    print("\n\n")

    # Grab Job Monitoring CSV
    df = pd.read_csv("%s/analyze_agg.csv" % din)

    # Pre-selection, basic filtering
    df["size"] = df["avg_pbr_size"]/pbytes 
    for x in range(1,7):
        df["total_recalled_%smth_pb"%x] = df["total_recalled_%smth"%x]/pbytes 
        df["total_deleted_%smth_pb"%x]  = df["total_deleted_%smth"%x]/pbytes 

   
    # Basic info about datafram
    print("\n### DataFrame Information ###\n")
    first_job_date = dt.datetime.fromtimestamp( df["firstAccess"].min() ).strftime("%Y-%m-%d")
    last_job_date  = dt.datetime.fromtimestamp( df["lastAccess"].max() ).strftime("%Y-%m-%d")
    print("    Job Time Range: %s to %s \n" % (first_job_date,last_job_date))
    tot_jobs = df["nJobsTot"].sum()
    print("    nJobs total:    %s \n" % tot_jobs )
    
    
    # Print table, summed accross all sites
    print("\n\n### Total [PB] Deleted and Recalled Over 1-6 Months \n")
    print("\t Month      Deleted    Recalled    Ratio \n")
    for x in range(1,7):
        tot_recalled = df["total_recalled_%smth_pb"%x].sum()
        tot_deleted  = df["total_deleted_%smth_pb"%x].sum()
        print("\t %i          %7.3f   %7.3f    %7.3f \n" % ( x, tot_deleted, tot_recalled, tot_deleted/tot_recalled ) )   

    # Print table, accross Tiers
    print("\n\n### Total [PB] Deleted and Recalled Over Data Tiers \n")
    print("\t Data Tier  ")
    df_agg = df.groupby("tier").agg({"nJobsTot":sum}).sort_values("nJobsTot",ascending=False).reset_index()
    tiers = df_agg["tier"]
    df_group = df.groupby("tier")
    for tier in tiers:
        print("\t %s " % tier)
        group = df_group.get_group(tier)
        for x in range(1,7):
            tot_recalled = group["total_recalled_%smth_pb"%x].sum()
            tot_deleted  = group["total_deleted_%smth_pb"%x].sum()
            tot_ratio = 0.0
            if tot_recalled>0:
                tot_ratio = tot_deleted/tot_recalled
            print("\t\t %s=Months;  %7.3f=Deleted;  %7.3f = Recalled;  %7.3f = Ratio " % (x, tot_deleted, tot_recalled, tot_ratio) )

            
    # Primary Datasets Information
    topX_primds = 30
    primds = np.unique(df.primds)
    print("\n   %s unique primary datasets\n" % len(primds))
    df_agg = df.groupby("primds").agg({"nJobsTot":sum}).sort_values("nJobsTot", ascending=False).reset_index()
    df_agg["percent"] = 100.0*df_agg["nJobsTot"]/tot_jobs
    print( df_agg.head(topX_primds) )

    # histogram, nJobs, by primary dataset
    bins = np.arange(0,2e3,20)
    with PdfPages('%s/h_nJobsTot__primds.pdf' % dout) as pdf:
        fig, ax = plt.subplots()
        plt.hist( x=np.clip(df_agg["nJobsTot"], bins[0], bins[-1]), bins=bins, log=True)
        plt.xlabel("nJobs")
        plt.ylabel("Frequency")
        plt.title("Number of Total Jobs, By Primary Datasets" )
        plt.grid(True)
        pdf.savefig()
        plt.close()

    # Dataset Information
    topX_datasets = 30
    datasets = np.unique(df.dataset)
    print("\n   %s unique datasets\n" % len(datasets))
    df_agg = df.groupby("dataset").agg({"nJobsTot":sum}).sort_values("nJobsTot", ascending=False).reset_index()
    df_agg["percent"] = 100.0*df_agg["nJobsTot"]/tot_jobs
    print( df_agg.head(topX_datasets) )

    # histogram, nJobs, by dataset
    bins = np.arange(0,2e3,20)
    with PdfPages('%s/h_nJobsTot__dataset.pdf' % dout) as pdf:
        fig, ax = plt.subplots()
        plt.hist( x=np.clip(df_agg["nJobsTot"], bins[0], bins[-1]), bins=bins, log=True)
        plt.xlabel("nJobs")
        plt.ylabel("Frequency")
        plt.title("Number of Total Jobs, By Datasets" )
        plt.grid(True)
        pdf.savefig()
        plt.close()

    # histogram, max_deltaT, by dataset
    df_agg = df.groupby("dataset").agg({"max_deltaT":max}).reset_index()
    bins = np.arange(0,400,5)
    with PdfPages('%s/h_max_deltaT__dataset.pdf' % dout) as pdf:
        fig, ax = plt.subplots()
        plt.hist( x=np.clip(df_agg["max_deltaT"]/(60*60*24.0), bins[0], bins[-1]), bins=bins, log=True)
        plt.xlabel("max deltaT [Days]")
        plt.ylabel("Frequency")
        plt.title("Maximum Delta T [Days] between successive dataset accesses" )
        plt.grid(True)
        pdf.savefig()
        plt.close()



def summary_jm(din, dout):

    # Grab Job Monitoring CSV
    df = pd.read_csv("%s/jm_agg.csv" % din)

    # Pre-selection, basic filtering
    df = df[ df["job_interval"]>0.0 ]
    df["job_interval_sec"]  = df["job_interval"]*interval
    
    # Basic info about datafram
    print("\n### DataFrame Information ###\n")
    first_job_date = dt.datetime.fromtimestamp( df["first_job_begin"].min() ).strftime("%Y-%m-%d")
    last_job_date  = dt.datetime.fromtimestamp( df["last_job_begin"].max() ).strftime("%Y-%m-%d")
    print("    Job Time Range: %s to %s \n" % (first_job_date,last_job_date))
    tot_jobs = df["nJobs"].sum()
    print("    nJobs total:    %s \n" % tot_jobs )
    h_intervals = np.unique(mdates.epoch2num(df.job_interval_sec))
    
    # Site Info
    print("\n### Site Information ###\n")
    sites = np.unique(df.job_site)
    print("   %s unique sites\n" % len(sites))
    print( sites )
    df_agg = df.groupby("job_site").agg({"nJobs":sum}).sort_values("nJobs", ascending=False).reset_index()
    df_agg["percent"] = 100.0*df_agg["nJobs"]/tot_jobs
    print("\n   Ranked Sites by Number of Jobs \n")
    print( df_agg )
    
    # Group data by T1, T2, T3
    df_t1 = df[df.job_site.str.contains('T1_')]
    df_t2 = df[df.job_site.str.contains('T2_')]
    df_t3 = df[df.job_site.str.contains('T3_')]
    df_by_tier = [ df_t1, df_t2, df_t3 ]

    tot_jobs_t1 = df_t1["nJobs"].sum()
    tot_jobs_t2 = df_t2["nJobs"].sum()
    tot_jobs_t3 = df_t3["nJobs"].sum()
    print("\n  Number of Jobs by Computing Tier \n")
    print("    nJobs at T1 sites: %s" % tot_jobs_t1 )
    print("    nJobs at T2 sites: %s" % tot_jobs_t2 )
    print("    nJobs at T3 sites: %s" % tot_jobs_t3 )
  
    # nJobs vs interval, T-X sites
    with PdfPages('%s/siteUsage_vs_interval__TXsites.pdf' % dout) as pdf:
        fig, ax = plt.subplots()
        labels, x_vals, y_vals = [], [], []
        for idx, idf in enumerate(df_by_tier):
            labels.append( "T%s Sites" % (idx+1) )
            x_vals.append( mdates.epoch2num(idf["job_interval_sec"]) )
            y_vals.append( idf["nJobs"] )
        plt.hist( x=x_vals, weights=y_vals, bins=h_intervals, label=labels, stacked=True )
        fig.autofmt_xdate()
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        plt.legend(loc="upper left")
        plt.xlabel("Date")
        plt.ylabel("Number of Jobs")
        plt.title("Number of Jobs vs Time, T-X sites")
        plt.grid(True)
        pdf.savefig()
        plt.close()

    # nJobs vs interval, top X sites 
    topX_sites = 5
    top_sites = df_agg["job_site"].head(topX_sites)
    with PdfPages('%s/siteUsage_vs_interval__Top%ssites.pdf' % (dout,topX_sites)) as pdf:
        fig, ax = plt.subplots()
        labels, x_vals, y_vals = [], [], []
        for site in top_sites:
            idf = df[df.job_site==site]
            labels.append( "%s" % site )
            x_vals.append( mdates.epoch2num(idf["job_interval_sec"]) )
            y_vals.append( idf["nJobs"] )
        plt.hist( x=x_vals, weights=y_vals, bins=h_intervals, label=labels, stacked=True )
        fig.autofmt_xdate()
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        plt.legend(loc="upper left")
        plt.xlabel("Date")
        plt.ylabel("Number of Jobs")
        plt.title("Number of Jobs vs Time, Top %s sites" % topX_sites )
        plt.grid(True)
        pdf.savefig()
        plt.close()

    # Dataset Tier Info
    print("\n### Dataset Information By Tier ###\n")
    df_agg = df.groupby("tier").agg({"nJobs":sum}).sort_values("nJobs", ascending=False).reset_index()
    df_agg["percent"] = 100.0*df_agg["nJobs"]/tot_jobs
    print("\n   Ranked Dataset Tiers by Number of Jobs \n")
    print( df_agg )
    tiers = df_agg["tier"]
    
    # nJobs vs interval, by tier
    with PdfPages('%s/siteUsage_vs_interval__tier.pdf' % dout) as pdf:
        fig, ax = plt.subplots()
        labels, x_vals, y_vals = [], [], []
        for tier in tiers:
            idf = df[df.tier==tier]
            labels.append( "%s" % tier )
            x_vals.append( mdates.epoch2num(idf["job_interval_sec"]) )
            y_vals.append( idf["nJobs"] )
        plt.hist( x=x_vals, weights=y_vals, bins=h_intervals, label=labels, stacked=True )
        fig.autofmt_xdate()
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        plt.legend(loc="upper left", fontsize="xx-small", ncol=2)
        plt.xlabel("Date")
        plt.ylabel("Number of Jobs")
        plt.title("Number of Jobs vs Time, By Dataset Tier"  )
        plt.grid(True)
        pdf.savefig()
        plt.close()

    # Primary Datasets Information
    topX_primds = 10
    primds = np.unique(df.primds)
    print("\n   %s unique primary datasets\n" % len(primds))
    df_agg = df.groupby("primds").agg({"nJobs":sum}).sort_values("nJobs", ascending=False).reset_index()
    df_agg["percent"] = 100.0*df_agg["nJobs"]/tot_jobs
    print("\n   Ranked Datasets by Number of Jobs \n")
    top_primds = df_agg["primds"].head(topX_primds)
    print( df_agg.head(topX_primds*3) )
   
    # nJobs vs interval, by primary dataset
    with PdfPages('%s/siteUsage_vs_interval__primds.pdf' % dout) as pdf:
        fig, ax = plt.subplots()
        labels, x_vals, y_vals = [], [], []
        for primds in top_primds:
            idf = df[df.primds==primds]
            labels.append( "%s" % primds )
            x_vals.append( mdates.epoch2num(idf["job_interval_sec"]) )
            y_vals.append( idf["nJobs"] )
        plt.hist( x=x_vals, weights=y_vals, bins=h_intervals, label=labels, stacked=True )
        fig.autofmt_xdate()
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        plt.legend(loc="upper left", fontsize="xx-small")
        plt.xlabel("Date")
        plt.ylabel("Number of Jobs")
        plt.title("Number of Jobs vs Time, By Top %s Primary Datasets"% topX_primds  )
        plt.grid(True)
        pdf.savefig()
        plt.close()


def summary_analyze_agg(din, dout):
    
    # Grab Job Monitoring CSV
    df = pd.read_csv("%s/analyze_agg.csv" % din)

    # Pre-selection, basic filtering
    df["size"] = df["avg_size"]/pbytes 
    for x in range(1,7):
        df["total_recalled_%smth_pb"%x] = df["total_recalled_%smth"%x]/pbytes 
        df["total_deleted_%smth_pb"%x]  = df["total_deleted_%smth"%x]/pbytes 

    tot_jobs = df["nJobsTot"].sum()
 
    # Print table, summed accross all sites
    print("\n\n### Total [PB] Deleted and Recalled Over 1-6 Months \n")
    print("\t Month      Deleted    Recalled    Ratio \n")
    for x in range(1,7):
        tot_recalled = df["total_recalled_%smth_pb"%x].sum()
        tot_deleted  = df["total_deleted_%smth_pb"%x].sum()
        print("\t %i          %7.3f   %7.3f    %7.3f \n" % ( x, tot_deleted, tot_recalled, tot_deleted/tot_recalled ) )   

    # Print table, accross Tiers
    print("\n\n### Total [PB] Deleted and Recalled Over Data Tiers \n")
    print("\t Data Tier  ")
    df_agg = df.groupby("tier").agg({"nJobsTot":sum}).sort_values("nJobsTot",ascending=False).reset_index()
    tiers = df_agg["tier"]
    df_group = df.groupby("tier")
    for tier in tiers:
        print("\t %s " % tier)
        group = df_group.get_group(tier)
        for x in range(1,7):
            tot_recalled = group["total_recalled_%smth_pb"%x].sum()
            tot_deleted  = group["total_deleted_%smth_pb"%x].sum()
            tot_ratio = 0.0
            if tot_recalled>0:
                tot_ratio = tot_deleted/tot_recalled
            print("\t\t %s=Months;  %7.3f=Deleted;  %7.3f = Recalled;  %7.3f = Ratio " % (x, tot_deleted, tot_recalled, tot_ratio) )


    # Primary Datasets Information
    topX_primds = 30
    primds = np.unique(df.primds)
    print("\n   %s unique primary datasets\n" % len(primds))
    df_agg = df.groupby("primds").agg({"nJobsTot":sum}).sort_values("nJobsTot", ascending=False).reset_index()
    df_agg["percent"] = 100.0*df_agg["nJobsTot"]/tot_jobs
    print( df_agg.head(topX_primds) )

    # histogram, nJobs, by primary dataset
    bins = np.arange(0,2e3,20)
    with PdfPages('%s/h_nJobsTot__primds.pdf' % dout) as pdf:
        fig, ax = plt.subplots()
        plt.hist( x=np.clip(df_agg["nJobsTot"], bins[0], bins[-1]), bins=bins, log=True)
        plt.xlabel("nJobs")
        plt.ylabel("Frequency")
        plt.title("Number of Total Jobs, By Primary Datasets" )
        plt.grid(True)
        pdf.savefig()
        plt.close()

    # Dataset Information
    topX_datasets = 30
    datasets = np.unique(df.dataset)
    print("\n   %s unique datasets\n" % len(datasets))
    df_agg = df.groupby("dataset").agg({"nJobsTot":sum}).sort_values("nJobsTot", ascending=False).reset_index()
    df_agg["percent"] = 100.0*df_agg["nJobsTot"]/tot_jobs
    print( df_agg.head(topX_datasets) )

    # histogram, nJobs, by dataset
    bins = np.arange(0,2e3,20)
    with PdfPages('%s/h_nJobsTot__dataset.pdf' % dout) as pdf:
        fig, ax = plt.subplots()
        plt.hist( x=np.clip(df_agg["nJobsTot"], bins[0], bins[-1]), bins=bins, log=True)
        plt.xlabel("nJobs")
        plt.ylabel("Frequency")
        plt.title("Number of Total Jobs, By Datasets" )
        plt.grid(True)
        pdf.savefig()
        plt.close()

    # histogram, max_deltaT, by dataset
    df_agg = df.groupby("dataset").agg({"max_deltaT":max}).reset_index()
    bins = np.arange(0,400,5)
    with PdfPages('%s/h_max_deltaT__dataset.pdf' % dout) as pdf:
        fig, ax = plt.subplots()
        plt.hist( x=np.clip(df_agg["max_deltaT"]/(60*60*24.0), bins[0], bins[-1]), bins=bins, log=True)
        plt.xlabel("max deltaT [Days]")
        plt.ylabel("Frequency")
        plt.title("Maximum Delta T [Days] between successive dataset accesses" )
        plt.grid(True)
        pdf.savefig()
        plt.close()



def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    
    #Check if output directory exists
    if not os.path.isdir(opts.dout):
        os.makedirs(opts.dout)

    if opts.agg_by_site:
        summary_jm_aggBySite(opts.din, opts.dout)
        summary_analyze_agg_aggBySite(opts.din, opts.dout)
    else: 
        summary_jm(opts.din, opts.dout)
        summary_analyze_agg(opts.din, opts.dout)


if __name__ == '__main__':
    main()
