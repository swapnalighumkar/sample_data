
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

class Solution:
    def __init__(self,spark,get_config(args.res_path, args.job_name)):
        self.primary_person_use_df=spark.read.csv(config['relative_path'] + Primary_Person_use.csv)
        
    def solution1():
        soln1= primary_person_use_df.filter(" (lower(PRSN_GNDR_ID)=='male') and (DEATH_CNT==1) ").select("CRASH_ID").distinct()
        return soln1.count()
        
