from operator import add
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

class Solution:
   
    def __init__(self,spark,src_path):
        '''Reading data from required files'''
        self.primary_person_use_df=spark.read.option("header","True").csv(src_path + 'Primary_Person_use.csv')
        self.Units_use_df=spark.read.option("header","True").csv(src_path + 'Units_use.csv')
        self.dmg=spark.read.option("header","true").csv(src_path + 'Damages_use.csv")
        self.n=Window.partitionBy("row").orderBy(desc("count"))
    
    def solution1(self):
        '''selecting unique crash_id after filtering data based on gender and death count'''
        soln1= self.primary_person_use_df.filter(" (lower(PRSN_GNDR_ID)=='male') and (DEATH_CNT==1) ").select("CRASH_ID").distinct()
        print("solution 1:",soln1.count())
        

    def solution2(self):
        '''count of total two wheelers '''
        soln2=self.Units_use_df.filter("upper(VEH_BODY_STYL_ID) in ('MOTORCYCLE','POLICE MOTORCYCLE') ")
        print("Solution 2:",soln2.count())
        
    
    def solution3(self):
        '''retrieving state name with highest number of accidents involving females   '''
        soln3=self.primary_person_use_df.filter(" (lower(DRVR_LIC_STATE_ID) NOT IN('other','na','unknown')) AND (PRSN_GNDR_ID=='FEMALE')").\
        groupBy("DRVR_LIC_STATE_ID").\
        count().sort(desc("count")).take(1)[0][0]
        
        print("Solution 3:",soln3)
        

    def solution4(self):
        '''selecting unique crash ids resulting into injury or death'''
        prsn_crsh_id=self.primary_person_use_df.filter("TOT_INJRY_CNT !=0 or DEATH_CNT!=0").select("CRASH_ID").distinct()
        '''join unique crash id with Units_use_df then filter the data'''
       
        prsn_join_units=prsn_crsh_id.join(self.Units_use_df,"CRASH_ID",'inner').filter("upper(VEH_MAKE_ID) NOT IN('OTHER (EXPLAIN IN NARRATIVE)','NA','UNKNOWN')").\
        select("CRASH_ID","VEH_MAKE_ID")

        ''''count the rows based on vehical make id then sort it in descending manner and add dummy column row'''
        veh_cnt=prsn_join_units.groupBy("VEH_MAKE_ID").agg(count("VEH_MAKE_ID").alias("count")).orderBy(desc("count")).withColumn("row",lit("1"))
       
        '''provide row number and fetch the result'''
        veh_order=veh_cnt.withColumn("row",row_number().over(self.n))
        soln4=veh_order.filter(veh_order.row.between(5,15)).select("VEH_MAKE_ID")
        print("Solution 4 :")
        soln4.show()
        
    def solution6(self):

        v1=self.Units_use_df.filter("upper(VEH_BODY_STYL_ID) In ('PASSENGER CAR, 2-DOOR','PASSENGER CAR, 4-DOOR','POLICE CAR/TRUCK')")
        v2=v1.filter("((upper(CONTRIB_FACTR_1_ID)=='UNDER INFLUENCE - ALCOHOL') or (upper(CONTRIB_FACTR_2_ID)=='UNDER INFLUENCE - ALCOHOL') \
        or (upper(CONTRIB_FACTR_P1_ID)=='UNDER INFLUENCE - ALCOHOL'))").select("CRASH_ID").distinct()
        join_res=v2.join(self.primary_person_use_df,on="CRASH_ID",how="inner").filter("(length(DRVR_ZIP)=5)").select("CRASH_ID","DRVR_ZIP").groupBy("DRVR_ZIP").\
        agg(count("*").alias("count")).orderBy(desc("count"))
       
        print("Solution 6 :")
        soln6=join_res.show(5)
        

    def solution7(self):
        dmg_df=self.dmg.select("CRASH_ID").distinct()
        prefil=self.Units_use_df.filter("((upper(VEH_DMAG_SCL_1_ID) in ('DAMAGED 6','DAMAGED 5','DAMAGED 7 HIGHEST')) or (upper(VEH_DMAG_SCL_2_ID) in \
        ('DAMAGED 6','DAMAGED 5','DAMAGED 7 HIGHEST')))")
        veh_fil=prefil.filter("((upper(FIN_RESP_TYPE_ID) in ('PROOF OF LIABILITY INSURANCE','LIABILITY INSURANCE POLICY')) and (upper(VEH_BODY_STYL_ID) in\
         ('PASSENGER CAR, 2-DOOR','PASSENGER CAR, 4-DOOR','POLICE CAR/TRUCK')))").select("CRASH_ID").distinct()
        soln7=veh_fil.subtract(dmg_df)
        print("Solution 7:",soln7.count())
        










        
