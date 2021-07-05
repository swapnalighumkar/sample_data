import argparse
import os
from pyspark.sql import SparkSession
import importlib
import sys
import json
from solution

if os.path.exists('solution.zip'):
    sys.path.insert(0, 'solution.zip')


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='My pyspark job arguments')
    parser.add_argument('--job', type=str, required=True, dest='job_name',
                        help='The name of the spark job you want to run')
    parser.add_argument('--res-path', type=str, required=True, dest='res_path',
                        help='Path to the solution resurces')
    
    args = parser.parse_args()
    src_path=args.res_path

    spark = SparkSession\
        .builder\
        .appName(args.job_name)\
        .getOrCreate()

    job_module = importlib.import_module(args.job_name)
    ob1=job_module.Solution(spark,src_path)
    answer1=ob1.solution1()
    answer2=ob1.solution2()
    answer3=ob1.solution3()
    answer4=ob1.solution4()
    answer6=ob1.solution6()
    answer7=ob1.solution7()


    print('[JOB {job} RESULT]: {result}'.format(job=args.job_name, result=res))
