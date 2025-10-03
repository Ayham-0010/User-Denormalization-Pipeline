def extract_ids_string(df, column_name):

    unique_df = df.select(column_name).dropDuplicates()
    

    rows = unique_df.collect()
    

    values_str = ', '.join([f"'{row[column_name]}'" for row in rows])
    
    return values_str


def user_denormalization (account_ids_string, spark):
    


    account_df = spark.sql(f"SELECT * FROM account WHERE id IN ({account_ids_string})").cache()
    account_type_df = spark.sql(f"SELECT * FROM account_type WHERE accountId  IN ({account_ids_string})").cache()
    
    individual_df = spark.sql(f"SELECT * FROM individual WHERE accountId  IN ({account_ids_string})").cache()

    individual_course_or_training_df = spark.sql(f"SELECT * FROM individual_course_or_training WHERE accountId  IN ({account_ids_string})").cache()
    individual_education_df = spark.sql(f"SELECT * FROM individual_education WHERE accountId  IN ({account_ids_string})").cache()
    individual_experience_df = spark.sql(f"SELECT * FROM individual_experience WHERE accountId  IN ({account_ids_string})").cache()


    account_df.createOrReplaceTempView("account_temp")
    account_type_df.createOrReplaceTempView("account_type_temp")
    individual_df.createOrReplaceTempView("individual_temp")


    individual_course_or_training_df.createOrReplaceTempView("individual_course_or_training_temp")
    individual_education_df.createOrReplaceTempView("individual_education_temp")
    individual_experience_df.createOrReplaceTempView("individual_experience_temp")




    courseId_df = spark.sql(f"SELECT courseId FROM individual_course_or_training")
    course_ids_str = extract_ids_string(courseId_df,'courseId')
    if course_ids_str != "":
        courses_df = spark.sql(f"SELECT * FROM courses WHERE id IN ({course_ids_str})").cache()
    else:
        courses_df = spark.sql(f"SELECT * FROM courses WHERE 1 = 0").cache()
    courses_df.createOrReplaceTempView("courses_temp")


    professionId_df = spark.sql(f"SELECT primaryProfessionId FROM individual")
    profession_ids_str = extract_ids_string(professionId_df,'primaryProfessionId')
    if profession_ids_str != "":
        professions_df = spark.sql(f"SELECT * FROM professions WHERE id IN ({profession_ids_str})").cache()
    else:
        professions_df = spark.sql(f"SELECT * FROM professions WHERE 1 = 0").cache()
    professions_df.createOrReplaceTempView("professions_temp")


    universityId_df = spark.sql(f"SELECT universityId FROM individual_education")
    university_ids_str = extract_ids_string(universityId_df,'universityId')
    if university_ids_str != "":
        universities_df = spark.sql(f"SELECT * FROM universities WHERE id IN ({university_ids_str})").cache()
    else:
        universities_df = spark.sql(f"SELECT * FROM universities WHERE 1 = 0").cache()
    universities_df.createOrReplaceTempView("universities_temp")    


    degreeId_df = spark.sql(f"SELECT degreeId FROM individual_education")
    degree_ids_str = extract_ids_string(degreeId_df,'degreeId')
    if degree_ids_str != "":
        degrees_df = spark.sql(f"SELECT * FROM degrees WHERE id IN ({degree_ids_str})").cache()
    else:
        degrees_df = spark.sql(f"SELECT * FROM degrees WHERE 1 = 0").cache()
    degrees_df.createOrReplaceTempView("degrees_temp")


    companyId_df = spark.sql(f"SELECT companyId FROM individual_experience")
    company_ids_str = extract_ids_string(companyId_df,'companyId')
    if company_ids_str != "":
        companies_df = spark.sql(f"SELECT * FROM companies WHERE id IN ({company_ids_str})").cache()
    else:
        companies_df = spark.sql(f"SELECT * FROM companies WHERE 1 = 0").cache()
    companies_df.createOrReplaceTempView("companies_temp") 


    employeeTypeId_df = spark.sql(f"SELECT employmentTypeId FROM individual_experience")
    employeeType_ids_str = extract_ids_string(employeeTypeId_df,'employmentTypeId')
    if employeeType_ids_str != "":
        employee_types_df = spark.sql(f"SELECT * FROM employee_types WHERE id IN ({employeeType_ids_str})").cache()
    else:
        employee_types_df = spark.sql(f"SELECT * FROM employee_types WHERE 1 = 0").cache()
    employee_types_df.createOrReplaceTempView("employee_types_temp")


    positionId_df = spark.sql(f"SELECT positionId FROM individual_experience")
    position_ids_str = extract_ids_string(positionId_df,'positionId')
    if position_ids_str != "":
        positions_df = spark.sql(f"SELECT * FROM positions WHERE id IN ({position_ids_str})").cache()
    else:
        positions_df = spark.sql(f"SELECT * FROM positions WHERE 1 = 0").cache()
    positions_df.createOrReplaceTempView("positions_temp")
        

    user_tag_df = spark.sql(f"SELECT * FROM user_tag WHERE account_id IN ({account_ids_string})").cache()
    user_tag_df.createOrReplaceTempView("user_tag_temp")


    tagId_df = spark.sql(f"SELECT tag_id FROM user_tag")
    tag_ids_str = extract_ids_string(tagId_df,'tag_id')
    if tag_ids_str != "":
        tags_df = spark.sql(f"SELECT * FROM tags WHERE id IN ({tag_ids_str})").cache()
    else:
        tags_df = spark.sql(f"SELECT * FROM tags WHERE 1 = 0").cache()
    tags_df.createOrReplaceTempView("tags_temp")




    user_df = spark.sql(f"""
    
    SELECT
    acc.id AS account_id,
        collect_list(DISTINCT struct(   acc.id,
                                        acc.status,
                                        a.firstName, 
                                        a.lastName,
                                        acct.type AS accountType,
                                        pro.name AS primaryProfessionName,
                                        acc.operationType AS account_operationType)) AS account_info,
                        

        collect_list(DISTINCT struct(   tag.id, 
                                        tag.name AS tagName, 
                                        tag.deleted_at,
                                        tag.operationType AS tag_operationType)) AS tags,



        collect_list(DISTINCT struct(   ct.id, 
                                        ct.accountId, 
                                        
                                        ct.courseId, 
                                        cou.name AS courseName, 
                                        cou.status AS courseStatus , 
                                        cou.operationType AS course_OperationType, 
                                        

                                        -- ct.deletedAt,
                                        
                                        ct.operationType AS individualCourseOrTraining_operationType)) AS individual_course_or_training,
                            


        collect_list(DISTINCT struct(   ed.id, 
                                        ed.accountId, 
                                        
                                        -- ed.courseId, 
                                        -- ecou.name AS courseName, 
                                        -- ecou.status AS courseStatus, 
                                        -- ecou.operationType AS course_OperationType,
                                        
                                        ed.degreeId, 
                                        deg.name AS degreeName, 
                                        deg.status AS degreeStatus, 
                                        deg.operationType AS degree_OperationType,
                                        
                                        ed.universityId, 
                                        uni.name AS universityName, 
                                        uni.status AS universityStatus, 
                                        uni.operationType AS university_OperationType,
                                        
                                        
                                        ed.operationType AS individualEducation_operationType)) AS individual_education,
                                        

        collect_list(DISTINCT struct(   ex.id, 
                                        ex.accountId, 

                                        ex.companyId,
                                        com.name AS companyName, 
                                        com.status AS companyStatus,
                                        com.operationType AS company_operationType, 
                                        
                                        ex.employmentTypeId, 
                                        emt.name AS employeeTypeName,
                                        emt.status AS employeeTypeStatus,
                                        emt.operationType AS employeeType_operationType,
                                        
                                        ex.positionId, 
                                        pos.name AS positionName,
                                        pos.status AS positionStatus,
                                        pos.operationType AS position_operationType,
                                        

                                        ex.operationType AS individualExperience_operationType)) AS individual_experience
                

   FROM
        account_temp acc
        

    
    LEFT JOIN
        account_type_temp acct ON acc.id = acct.accountId
        
    LEFT JOIN
        individual_temp a ON acc.id = a.accountId
    
    LEFT JOIN
        professions_temp pro ON a.primaryProfessionId = pro.id
        
                        
    LEFT JOIN
        individual_course_or_training_temp ct ON a.accountId = ct.accountId
    LEFT JOIN
        courses_temp cou ON ct.courseId = cou.id


                                               
    LEFT JOIN
        individual_education_temp ed ON a.accountId = ed.accountId
    -- LEFT JOIN
        -- courses_temp ecou ON ed.courseId = ecou.id
    LEFT JOIN
        universities_temp uni ON ed.universityId = uni.id
    LEFT JOIN
        degrees_temp deg ON ed.degreeId = deg.id
        

                        
    LEFT JOIN
        individual_experience_temp ex ON a.accountId = ex.accountId
                        
    LEFT JOIN
        companies_temp com ON ex.companyId = com.id
    LEFT JOIN
        employee_types_temp emt ON ex.employmentTypeId = emt.id   
    LEFT JOIN
        positions_temp pos ON ex.positionId = pos.id 





    LEFT JOIN
        user_tag_temp ut ON acc.id = ut.account_id
    
    LEFT JOIN
        tags_temp tag ON ut.tag_id = tag.id
    
  
    

    WHERE
         acc.id IN ({account_ids_string}) -- AND acct.type = "individual"
    
         
    GROUP BY
        acc.id
        
    """)

    user_df.show()
    return user_df


 

