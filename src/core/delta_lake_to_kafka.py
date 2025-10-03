from pyspark.sql import functions as F
from pyspark.sql.functions import get_json_object, col,from_json, to_json, struct
from .logger import logger

def cdc_delta_table(table_name, id_key ,table_commit_version, CDC_bacth_size, spark):
    commit_version = table_commit_version

    user_data_list = []


    try:

        df_cdc = spark.sql(f"""
        SELECT _commit_version, {id_key} AS id  FROM table_changes('{table_name}', {commit_version + 1}, {commit_version + CDC_bacth_size})
        
        """)
    
        max_commit_version = df_cdc.agg(F.max("_commit_version")).collect()[0][0]
    
        if max_commit_version is None:
           
            commit_version += 1
            logger.info(f"++")
        else:
            logger.info(f"MAX ({max_commit_version})" )
            commit_version = max_commit_version
            
        df_cdc.show()
        new_data = df_cdc.collect() 

        if table_name in [ "account", "account_type", "location_account", "profile_image", "individual","individual_course_or_training", "individual_education", "individual_experience","individual_language", "individual_skill", "organization", "organization_section","user_tag"]:
            user_data_list.extend(new_data)


        return commit_version ,user_data_list
        
    except Exception as e:
        print(f"Exception occurred: {e}")

        return commit_version ,user_data_list




def covert_ids_data_list_to_string (data_list):
    ids = [f"'{row.id}'" for row in data_list]
    ids_string = ", ".join(ids)
    return ids_string
    




def convert_to_kafka_message(key,dataframe):
    
    df = dataframe.withColumn("value", to_json(struct([col(c) for c in dataframe.columns if c != f'{key}']))).selectExpr(f"{key} as key", "value")
    return df



def write_to_kafka(dataframe, topic_name , kafka_server):
    dataframe.write\
      .format("kafka")\
      .option("kafka.bootstrap.servers", kafka_server)\
      .option("topic", topic_name) \
      .save()


