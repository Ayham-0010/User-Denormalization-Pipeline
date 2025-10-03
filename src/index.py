
import json
import os
import time
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import get_json_object, col,from_json, to_json, struct
from pyspark.sql import functions as F


from delta import *


from core.logger import logger
from core.denormalization_sql import *
from core.delta_lake_to_kafka import *




def main():
    logger.info("service started successfully")


    try:


        kafka_servers = os.environ['KAFKA_BOOTSTRAP_SERVERS']
        kafka_topic = os.environ['OUTPUT_KAFKA_TOPIC']



        logger.info("creating spark session")
        spark = (
                SparkSession.builder
                .appName("user-denormalization-app")
                .config("spark.streaming.stopGracefullyOnShutdown", True)
                .getOrCreate()
            )

        base_path = os.environ['S3_USER_PATH']
        

        paths = [
            "account/", "account_type/", "location_account/", "profile_image/", "individual/",
            "individual_course_or_training/", "individual_education/", "individual_experience/",
            "individual_language/", "individual_skill/", "organization/", "organization_section/",
            "businesses/", "companies/", "locations/", "courses/", "degrees/",
            "employee_types/", "help_types/", "countries/", "honorifics/", "policies/",
            "positions/", "professions/", "specializations/", "tags/", "user_tag/",
            "universities/", "states/", "languages/"
        ]


        table_names = [
            "account", "account_type", "location_account", "profile_image", "individual",
            "individual_course_or_training", "individual_education", "individual_experience",
            "individual_language", "individual_skill", "organization", "organization_section",
            "businesses", "companies", "locations", "courses", "degrees",
            "employee_types", "help_types", "countries", "honorifics", "policies",
            "positions", "professions", "specializations", "tags", "user_tag",
            "universities", "states", "languages"
        ]


        logger.info("enabling CDC in new delta tables")
        spark.sql(f"set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;")

        for path, table_name in zip(paths, table_names):
            try:
                spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{base_path + path}'")
                logger.info(f"{table_name} table created at'{base_path + path}'")
            except:
                logger.error(f"Failed to create the table {table_name} at '{base_path + path}'")
                continue


      

        #check_point
        base_path = os.environ['S3_CHECKPOINT_PATH']



        individual_checkpoint_delta_tables_path = "individuals/"
        spark.sql(f"CREATE TABLE IF NOT EXISTS individual_checkpoint USING DELTA LOCATION '{base_path + individual_checkpoint_delta_tables_path}'")
        logger.info(f"individual_checkpoint table created")

        individual_course_or_training_checkpoint_delta_tables_path = "individual_course_or_training/"
        spark.sql(f"CREATE TABLE IF NOT EXISTS individual_course_or_training_checkpoint USING DELTA LOCATION '{base_path + individual_course_or_training_checkpoint_delta_tables_path}'")
        logger.info(f"individual_course_or_training_checkpoint table created")

        individual_education_checkpoint_delta_tables_path = "individual_education/"
        spark.sql(f"CREATE TABLE IF NOT EXISTS individual_education_checkpoint USING DELTA LOCATION '{base_path + individual_education_checkpoint_delta_tables_path}'")
        logger.info(f"individual_education_checkpoint table created")

        individual_experience_checkpoint_delta_tables_path = "individual_experience/"
        spark.sql(f"CREATE TABLE IF NOT EXISTS individual_experience_checkpoint USING DELTA LOCATION '{base_path + individual_experience_checkpoint_delta_tables_path}'")
        logger.info(f"individual_experience_checkpoint table created")
   
        user_tag_checkpoint_delta_tables_path = "user_tag/"
        spark.sql(f"CREATE TABLE IF NOT EXISTS user_tag_checkpoint USING DELTA LOCATION '{base_path + user_tag_checkpoint_delta_tables_path}'")
        logger.info(f"user_tag_checkpoint table created")



        CDC_bacth_size = int(os.environ['CDC_BATCH_SIZE'])
        batch_timeout_seconds = int(os.environ['TIME_WINDOW_IN_SECONDS'])



        individual_commit_version = spark.sql(f"select * from individual_checkpoint limit 1;").collect()[0][0]
        individual_course_or_training_commit_version = spark.sql(f"select * from individual_course_or_training_checkpoint limit 1;").collect()[0][0]
        individual_education_commit_version = spark.sql(f"select * from individual_education_checkpoint limit 1;").collect()[0][0]
        individual_experience_commit_version = spark.sql(f"select * from individual_experience_checkpoint limit 1;").collect()[0][0]

        user_tag_commit_version = spark.sql(f"select * from user_tag_checkpoint limit 1;").collect()[0][0]


        

        while True:


            time.sleep(batch_timeout_seconds)

            full_user_data_list = []
            user_data_list = []


            
             
            logger.info(f"cdc_delta_table: individual")
            ##
            individual_commit_version, user_data_list = cdc_delta_table('individual', 'accountId' ,individual_commit_version, CDC_bacth_size, spark)
            full_user_data_list.extend(user_data_list)
            user_data_list = []
                        
            individual_course_or_training_commit_version, user_data_list = cdc_delta_table('individual_course_or_training', 'accountId' ,individual_course_or_training_commit_version, CDC_bacth_size, spark)
            full_user_data_list.extend(user_data_list)
            user_data_list = []
                        
            individual_education_commit_version, user_data_list = cdc_delta_table('individual_education', 'accountId' ,individual_education_commit_version, CDC_bacth_size, spark)
            full_user_data_list.extend(user_data_list)
            user_data_list = []
                        
            individual_experience_commit_version, user_data_list = cdc_delta_table('individual_experience', 'accountId' ,individual_experience_commit_version, CDC_bacth_size, spark)
            full_user_data_list.extend(user_data_list)
            user_data_list = []

            user_tag_commit_version, user_data_list = cdc_delta_table('user_tag', 'account_id' ,user_tag_commit_version, CDC_bacth_size, spark)
            full_user_data_list.extend(user_data_list)
            user_data_list = []
            ##    

            time.sleep(5)

            


            logger.info(f"full_user_data_list: {full_user_data_list}")

            
            if full_user_data_list:

                logger.info(f"removing duplicates: {full_user_data_list}" )

                full_user_data_list = list(dict.fromkeys(full_user_data_list))

                
                
                logger.info("user denormalization started")

                users_ids_string = covert_ids_data_list_to_string(full_user_data_list)
                user_df = user_denormalization(users_ids_string, spark)

                logger.info("converting to kafka message started")
                userkafkaDF = convert_to_kafka_message("account_id", user_df)

                logger.info("writing to kafka started")
                write_to_kafka(userkafkaDF,kafka_topic,kafka_servers)


                
                spark.sql(f"UPDATE individual_checkpoint SET individual_ckp = {individual_commit_version};")
                spark.sql(f"UPDATE individual_course_or_training_checkpoint SET individual_course_or_training_ckp = {individual_course_or_training_commit_version};")
                spark.sql(f"UPDATE individual_education_checkpoint SET individual_education_ckp = {individual_education_commit_version};")
                spark.sql(f"UPDATE individual_experience_checkpoint SET individual_experience_ckp = {individual_experience_commit_version};")
                spark.sql(f"UPDATE user_tag_checkpoint SET user_tag_ckp = {user_tag_commit_version};")
                
                


                full_user_data_list = []
                logger.info("done")


    
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    main()
