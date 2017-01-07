#import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import numpy as np
import logging
import sys
import math
import enum
from enum import Enum
import time


#helper functions below
#===================================
def assign_tf_val(col):
    #This function assigns an integer to the 't'/'f' strings
    col_vals = col.values
    new_ar = []
    for jk in range(len(col_vals)):
        val = col_vals[jk]
        if val == 'Y':
            new_ar.append(True)
        elif val == 'N':
            new_ar.append(False)
        else:
            new_ar.append(val)
    return new_ar

def first_char(col):
    col_vals = col.values
    new_ar = []
    for jk in range(len(col_vals)):
        val = col_vals[jk]
        val = val[0][0]
        new_ar.append(val)
    return np.array(new_ar)

def convert_ac_year(df, return_df = False):
    '''converts to spring year'''
    col_vals = df['academic_year'].values
    new_ar = []
    for jk in range(len(col_vals)):
        val = int(col_vals[jk].split('-')[1])
        new_ar.append(val)
        
    if return_df:
        df['academic_year'] = new_ar
        return df
    else:
        return new_ar
    
def attendance_function(df):
    
    df = convert_ac_year(df, return_df = True)
    df.academic_year = df.academic_year.astype(int)
    
    df['d'] = df['academic_year']-1
    df.d = df.d.astype('str')
    df['period_start'] = '9/1/'+ df.d
    #df.drop(['d'], axis = 1, inplace= True)

    df['d'] = df['academic_year']
    df.d = df.d.astype('str')
    df['period_end'] = '6/30/'+ df.d
    df.drop(['d'], axis = 1, inplace= True)

    df['possible'] = 180

    return df

def clean_date(date):
    d = date.split('/')
    yr = d[-1]
    yr = '20'+yr
    
    return d[0]+'/'+d[1]+'/'+yr

def clean_grade(df_detail, st_id, ac_year):
    gl = df_detail.grade_level.values
    condition = (df_detail.student_id.values == st_id) & (df_detail.academic_year == ac_year)
    condition = condition.values
    grade = gl[condition]
    if len(grade)>0:
        grade = grade.astype(int)[0]
    else:
        grade = -1
    return grade

def clean_advance_retain(df_detail, st_id, ac_year):
    gl = df_detail.grade_level.values
    ac_year = ac_year.astype(int)
    
    
    condition1 = (df_detail.student_id.values == st_id) & (df_detail.academic_year == ac_year)
    condition2 = (df_detail.student_id.values == st_id) & (df_detail.academic_year == ac_year+1)
    

    condition1 = condition1.values
    condition2 = condition2.values

    grade_lower = gl[condition1]
    grade_higher = gl[condition2]
    
    if len(grade_lower)>=1 and len(grade_higher)>=1:
        grade_lower = grade_lower.astype(int)[0]
        grade_higher = grade_higher.astype(int)[0]
        if grade_lower+1 == grade_higher:
            status = 'advance'
        elif grade_lower == grade_higher:
            status = 'retain'
        else:
            status = 'other'
    else:
        status = 'other'
            
    return status

def clean_exit(df_detail, st_id, ac_year):
    #change code of second entry when there is an exit
    condition1 = (df_detail.student_id.values == st_id) & (df_detail.academic_year == ac_year) & (df_detail.code == 'other')
    
    if len(df_detail[condition1])>0:
        return df_detail[condition1].index.values[0]
    else:
        return -1
    

#data into correct format for database
#===================================

## Student schema
def clean_detail_for_student_schema(data_files, cleaned):
    """Modified from function for VPS for Arlington data."""
    """I want to build the student data here.  I think all of the information I need for this table is in the detail csv"""

    column_names = {
        'STUDENT_ID':          'student_id',
        'FIRST_TIME_9TH_GRADER_COHORT':      'cohort', #this seems to be the year that they finished 9th grade
        'BIRTH_DT':         'date_of_birth',
        'GENDER_DESC':           'gender',
        'ALT_RACE_DESC':     'race_ethnicity',
        'SPED': 'sped_any'

        }

    logging.info("ETL: Starting Arlington data cleaning for student schema...")

    
    for raw_base in data_files:

        df = pd.read_csv(raw_base, sep=',', header=0, dtype=object)
        logging.info("ETL:    Cleaning base data file raw_base %s"%raw_base)
        df.rename(columns=column_names, inplace=True)
        df = df.drop(['ETl_school_year', 
                      'SCHOOL_ID', 
                      'SCHOOL_LONG_NAME' ,
                      'GRADE_LEVEL_CD' , 
                      'FRL' ,'LEP' ,
                      'Total_Absence' ,
                      'Total_Tardies', 
                      'OSS_SUSP_NUMBERS', 
                      'graduate_ind' ,
                      'diploma_completer_ind', 
                      'First_Entry_Code', 
                      'First_Entry_Code_Desc', 'Diploma_Type'], axis=1)
        
        #This gives multiple entries per student. It would be a good idea to 
        #not replicate, so I'll first check to make sure it's consistent and then drop rows.
        df.fillna(-3, inplace = True)
        df.drop_duplicates(['student_id', 
                            'cohort', 
                            'race_ethnicity', 
                            'gender', 
                            'sped_any'
                           ], inplace = True)

        df.student_id = df.student_id.astype(int)
        
        df.cohort = df.cohort.astype(int) + 3 # convert to graduating cohort
        
       
        stID_counts = df['student_id'].value_counts()
        duplicate_stIDs = stID_counts[stID_counts>1].index.values
      
        sped = assign_tf_val(df.sped_any)
        df.sped_any = sped

       
        for j in range(len(duplicate_stIDs)):
            duplicate_stID = duplicate_stIDs[j]
        
            vv = df[df['student_id']==duplicate_stID]['sped_any'].values 
            if vv[0] or vv[1]:
                df.loc[df.student_id == duplicate_stIDs[j], 'sped_any'] = True
               
        df.drop_duplicates(['student_id', 
                            'cohort', 
                            'race_ethnicity', 
                            'gender', 
                            'sped_any'
                           ], inplace = True)
        
        race = first_char(df.race_ethnicity)
        df.race_ethnicity = race
        df.race_ethnicity = df.race_ethnicity.astype(Enum)
        df.loc[df.race_ethnicity == 'O', 'race_ethnicity'] = 'X'
        gen = first_char(df.gender)
        df.gender = gen
        df.gender = df.gender.astype(Enum)

        df.to_csv(cleaned, header=True, index=False, date_format='%Y%m%d')
        
#Enrollment scehma
def clean_detail_for_enrollment_schema(data_files, cleaned):
    column_names = {
        'STUDENT_ID':          'student_id',
        'FIRST_TIME_9TH_GRADER_COHORT':      'cohort', #this seems to be the year that they finished 9th grade
        'GENDER_DESC':           'gender',
        'ALT_RACE_DESC':     'race_ethnicity',
        'SPED': 'special_ed',
        'ETl_school_year': 'academic_year',
        'GRADE_LEVEL_CD' : 'grade_level',
        'LEP': 'ell', #limited english proficiency -> English language learner
       

        }

    logging.info("ETL: Starting Arlington cleaning for enrollment schema")
 
    base_dfs = []
    for raw_base in data_files:

        df = pd.read_csv(raw_base, sep=',', header=0, dtype=object)
        logging.info("ETL:    Cleaning base data file raw_base %s"%raw_base)
        df.rename(columns=column_names, inplace=True)

        #TT, 77, and GD seem to indicate that someone has gone on after 12th grade without graduating
        #I'm going to enter all as 77 since it's an int
        df.grade_level[df.grade_level == 'TT'] = '77'
        df.grade_level[df.grade_level == 'GD'] = '77'
        
        df = df.drop(['SCHOOL_ID', 
                      'SCHOOL_LONG_NAME' ,
                      'FRL' , #I'm not sure what this is
                      'Total_Absence' ,
                      'Total_Tardies', 
                      'OSS_SUSP_NUMBERS', 
                      'graduate_ind' ,
                      'diploma_completer_ind', 
                      'First_Entry_Code', 
                      'First_Entry_Code_Desc', 
                      'Diploma_Type', 
                      'race_ethnicity',
                      'cohort',
                      'gender'
                         
                     ], axis=1)
        
        #academic year should be an int.  We go with the spring year
        years = convert_ac_year(df)
        df['academic_year'] = years
        
        #convert data types for columns we have
        df.student_id = df.student_id.astype(int)
        df.grade_level= df.grade_level.astype(int)
        df.academic_year = df.academic_year.astype(int)
        ell = assign_tf_val(df.ell)
        df.ell = ell
        
        sped = assign_tf_val(df.special_ed)
        df.special_ed = sped
        
        
        df.d = df.academic_year -1
        df.d = df.d.astype('str')
        df['date'] = '9/1/'+ df.d
    
        pd.to_datetime(df.date, format='%m/%d/%Y', errors='coerce')
        
        df.to_csv(cleaned, header=True, index=False, date_format='%Y%m%d')

#Course, course enrollment, and school schema      
def clean_for_course_schema(course_data, detail_dat, cleaned):
    #I think I have to infer this from the marks data
    #I should get the student ID and academic year

    column_names = {
        'COURSE_CD':'code',
        'COURSE_LONG_DESC':'name',
        'SCHOOL_SHT_NAME':'school_name',
        'STUDENT_ID':          'student_id',
        'FIRST_TIME_9TH_GRADER_COHORT':      'cohort', #this seems to be the year that they finished 9th grade
        'BIRTH_DT':         'date_of_birth',
        'GENDER_DESC':           'gender',
        'ALT_RACE_DESC':     'race_ethnicity',
        'SPED': 'sped_any',
        'ETl_school_year':'academic_year',
        'ETL_SCHOOL_YEAR':'academic_year',
        'SCHOOL_ID':'school_id',
        'SCHOOL_LONG_NAME': 'school_name',
        'SCHOOL_SHT_NAME':'school_name',
        'COURSE_CD':'code',
        'COURSE_LONG_DESC' : 'desc',
        'Mark': 'mark',

         #need school ID.  will have to write something to look this up --> Can get from detail dat

        }


    #first create map for school name
    for raw_base in detail_dat:

        df_detail = pd.read_csv(raw_base, sep=',', header=0, dtype=object)
        
        
        df_detail.rename(columns=column_names, inplace=True)
        df_detail = df_detail.drop([
                      #'GRADE_LEVEL_CD',
                      'gender',
                      'cohort',
                      'race_ethnicity',
                      'FRL',
                      'LEP',
                      'sped_any',
                      'Total_Absence',
                      'Total_Tardies',
                      'OSS_SUSP_NUMBERS',
                      'graduate_ind',
                      'diploma_completer_ind',
                      'First_Entry_Code',
                      'First_Entry_Code_Desc',
                      'Diploma_Type',
                       
                     ], axis=1)
        
        #I'm going to make the school table.
        #There isn't enough data to get the years for each school from the data. 
        #extra research needed
        
        
        df_school = df_detail.drop_duplicates([
                             'school_name'
                            ], inplace = False)
        df_school = df_school.drop(['student_id',
                                   'academic_year',
                                   'GRADE_LEVEL_CD'],
                                   axis = 1
                                  )
        
        column_names_school = {
            'school_name':'name'
        }
        df_school.rename(columns=column_names_school, inplace=True)

        df_school_unknown = pd.DataFrame([[-1, 'unknown_1'], [-2, 'ND'], [-3,'APT']], columns =['school_id', 'name'])
        df_school = df_school.append(df_school_unknown)
 
        df_school.to_csv(cleaned[2], header=True, index=False, date_format='%Y%m%d')
      
    #this was mapped manually
    #we don't have course data for most of the schools.  
    #I'm going to map the numbers in this table and then map the names to the names from the other table
    school_to_id = {'H-B Woodlawn':'39',
               'CC':'70',
               'Williamsburg':'45',
               'Washington Lee':'44',
               'Yorktown':'49',
               'Wakefield':'43',
               'Arlington Mill':'26',
               'Jefferson':'23',
               'Swanson':'40',
               'Langston':'25',
               'Kenmore':'24',
               'Gunston':'15',
                'TJHSST':'96',
               'NaN':'-1',
               'ND':'-2',
                'APT':'-3'}
    
    for raw_base in course_data:

        df = pd.read_csv(raw_base, sep=',', header=0, dtype=object)
        df.rename(columns=column_names, inplace=True)
        df.student_id = df.student_id.astype(int)
        
        logging.info("ETL:    Cleaning base data file raw_base %s"%raw_base)
        logging.info("Cleaning data for course schema...")
        df.rename(columns=column_names, inplace=True)

        
        column_names = {'desc':'name', 'code':'course_code'}

        df['school_id'] = df['school_name'].map(school_to_id)
        #no foreign key constraint on school_id so just leave it in there for now.
        df.school_id.fillna(-1, inplace = True)
      

        df.rename(columns=column_names, inplace=True)
        years = convert_ac_year(df)
       
        years = np.array(years)
        df['d'] = years
        df.d = df.d.astype(int)
        
        #there is no info about dates besides academic year, so i'll just assume all course data is available on the last day of
        #the academic year.
        df.d = years
        df['academic_year'] = df.d.astype(int)
        df.d = df.d.astype('str')
        df['date'] = '6/30/'+ df.d
        df.drop(['d'], axis = 1, inplace= True)
        df_course_enrollment = df.drop(['name', 'school_name'], axis = 1)

        df_course_enrollment.drop_duplicates(['student_id',
                                             'academic_year',
                                             'course_code',
                                             'mark'], inplace = True)

        #Sometimes people will withdraw and re-enter the same year.
        #I will make the 'date' for the W the fall semester.
        withdrawn = df_course_enrollment[df_course_enrollment['mark']=='W']

        #below should be rewritten        
        start_time = time.time()
        for jk in range(len(withdrawn)):
            vv = withdrawn.iloc[jk]
            st_id = vv.student_id
            c_id = vv.course_code
            yr = vv.academic_year
            b = (df_course_enrollment['student_id'].values == st_id) & (df_course_enrollment['course_code'].values == c_id)

            vals = df_course_enrollment[b]
            if len(vals)>1:
                vals2 = vals[vals['mark']!='W']
                vw = vals[vals['mark']=='W']
                if yr in vals.academic_year.values:
                    indw = vw.index.values[0]
                    ind = vals[vals['academic_year']==yr].index.values[0]
                    new_year = df_course_enrollment.academic_year.loc[ind]-1
                    df_course_enrollment.date.loc[indw] = '12/31/'+ new_year.astype('str')
    
        end_time = time.time()
        
        #it looks like some people take the same course more than once in a year without withdrawing.
        #for now I'm going to drop this info
    
        df_course_enrollment.drop_duplicates(['student_id',
                                             'academic_year',
                                             'course_code',
                                             'date'], inplace = True)

        df_course_enrollment.to_csv(cleaned[1], header=True, index=False, date_format='%Y%m%d')
        
        #now I will reduce this course enrollment data into course data
        df_course_enrollment = df_course_enrollment.drop(['student_id', 
                      'academic_year', 
                      'mark' ,
                      'date'], axis=1)
        
        df_course_enrollment.drop_duplicates(['school_id',
                                             'course_code'], inplace=True)
        #column_names = {'code':'course_code'}
        #df_course_enrollment.rename(columns=column_names, inplace=True)
        df_course_enrollment['code'] = df_course_enrollment['course_code']
        df_course_enrollment.drop(['course_code'],axis = 1, inplace=True)
        
        df_course_enrollment.to_csv(cleaned[0], header=True, index=False, date_format='%Y%m%d')
        
def clean_for_attendance(detail_dat, cleaned):
    #for possible I will just be using 180 for each academic year
    #need: student_id, school_id, period_start, and period_end
    column_names = {
        'COURSE_CD':'code',
        'COURSE_LONG_DESC':'name',
        'SCHOOL_SHT_NAME':'school_name',
        'STUDENT_ID':          'student_id',
        'FIRST_TIME_9TH_GRADER_COHORT':      'cohort', #this seems to be the year that they finished 9th grade
        'BIRTH_DT':         'date_of_birth',
        'GENDER_DESC':           'gender',
        'ALT_RACE_DESC':     'race_ethnicity',
        'SPED': 'sped_any',
        'ETl_school_year':'academic_year',
        'ETL_SCHOOL_YEAR':'academic_year',
        'SCHOOL_ID':'school_id',
        'SCHOOL_LONG_NAME': 'school_name',
        'SCHOOL_SHT_NAME':'school_name',
        'COURSE_CD':'code',
        'COURSE_LONG_DESC' : 'desc',
        'Mark': 'mark',
        'Total_Tardies':'tardy',
        'Total_Absence':'absence_excused'

         #need school ID.  will have to write something to look this up --> Can get from detail dat

        }

 

    #first create map for school name
    for raw_base in detail_dat:
        

        df = pd.read_csv(raw_base, sep=',', header=0, dtype=object)

        df.rename(columns=column_names, inplace=True)
        df = df.drop([
                      'GRADE_LEVEL_CD',
                      'gender',
                      'cohort',
                      'race_ethnicity',
                      'FRL',
                      'LEP',
                      'sped_any',
                      'graduate_ind',
                      'diploma_completer_ind',
                      'First_Entry_Code',
                      'First_Entry_Code_Desc',
                      'Diploma_Type',
                      'OSS_SUSP_NUMBERS',
                      'school_name'
                       
                     ], axis=1)
        
       
        df_attendance = attendance_function(df)
        df_attendance.to_csv(cleaned, header=True, index=False, date_format='%Y%m%d')
        
def simplified_outcome_detail_code(outcome_dat, detail_dat):
    #for possible I will just be using 180 for each academic year
    #need: student_id, school_id, period_start, and period_end
    column_names = {
        'COURSE_CD':'code',
        'COURSE_LONG_DESC':'name',
        'SCHOOL_SHT_NAME':'school_name',
        'STUDENT_ID':          'student_id',
        'FIRST_TIME_9TH_GRADER_COHORT':      'cohort', #this seems to be the year that they finished 9th grade
        'BIRTH_DT':         'date_of_birth',
        'GENDER_DESC':           'gender',
        'ALT_RACE_DESC':     'race_ethnicity',
        'SPED': 'sped_any',
        'ETl_school_year':'academic_year',
        'ETL_SCHOOL_YEAR':'academic_year',
        'SCHOOL_ID':'school_id',
        'SCHOOL_LONG_NAME': 'school_name',
        'SCHOOL_SHT_NAME':'school_name',
        'COURSE_CD':'code',
        'COURSE_LONG_DESC' : 'desc',
        'Mark': 'mark',
        'Total_Tardies':'tardy',
        'Total_Absence':'absence_excused',
        'GRADE_LEVEL_CD':'grade_level'

         #need school ID.  will have to write something to look this up --> Can get from detail dat

        }

    df_detail = pd.read_csv(detail_dat, sep=',', header=0, dtype=object)
    df_outcome = pd.read_csv(outcome_dat, sep=',', header =0, dtype = object)

    
    df_outcome.rename(columns=column_names, inplace=True)
    df_detail.rename(columns = column_names, inplace=True)
        
    
    #we have more outcome data than we have student ID data.  
    #After some investigation I find that some students must have more than one outcome
    
    df_outcome = convert_ac_year(df_outcome, return_df = True)
    df_detail = convert_ac_year(df_detail, return_df = True)
    
    df_outcome = df_outcome.drop(['Year_Type',
                                 'school_name',
                                 #'grade_level', #this is not accurate in the outcome file.  It is always listed as 12 even though it isn't
                                 'race_ethnicity',
                                 'gender',
                                 'Entry_Date',
                                 'ENTRY_REASON_CD',
                                 
                                 ],
                                axis = 1)
 
    df_outcome.grade_level = -1
    df_detail = df_detail.drop(['school_name',
                                 'race_ethnicity',
                                 'gender',
                               'FRL',
                               'LEP',
                               'sped_any',
                               'absence_excused',
                                'tardy',
                                'cohort',
                                'OSS_SUSP_NUMBERS',
                                'First_Entry_Code_Desc',
                                'Diploma_Type',
                                'diploma_completer_ind',
                                'First_Entry_Code',
                              
                               ],
                                axis = 1)
   

    df_detail['code'] = 'end_of_year'#'end_of_year'
    df_detail['code'][df_detail['graduate_ind']=='1'] = 'graduate'
    df_detail = df_detail.drop(['graduate_ind'], axis = 1)
    
        
    df_detail = df_detail.sort(['student_id', 'grade_level'])
    df_detail['at_default_school'] = True #I will set this to True for the detail df
    df_outcome = df_outcome.sort(['student_id'])
    df_outcome['at_default_school'] = True #Just set this to false for now and will check later
    df_code = df_outcome.copy()
    df_outcome = df_outcome.drop(['EXIT_CODE_LONG_DESCRIPTION'], axis=1)
    df_code = df_code.drop(['academic_year',
                        
                           'student_id',
                           'school_id',
                           'Exit_Date',
                           ], axis =1)
    
    df_code = df_code.drop_duplicates(['EXIT_REASON_CD'])


    return df_detail, df_outcome

def clean_for_outcome(outcome_dat, detail_dat, cleaned):    
    APScode_to_standard = {'W503':'exit',
                           'W310':'exit',
                           'W970':'exit',
                           'W402':'exit',
                           'W313':'exit',
                           'W870':'exit',
                           'W321':'exit',
                           'W217':'exit',
                           'W306':'exit', 
                           'W307':'exit',
                           'W305':'exit',
                           'W304':'exit',
                           'W312':'exit',
                           'W960':'exit',
                           'W4TJ':'exit',
                           'W880':'dropout',
                           'W201':'transfer_internal',
                           'W503':'exit',
                           'W016':'transfer_internal',
                           'W115':'transfer_internal',
                           'W99': 'end_of_year_outcome', 
                           'W411':'other',
                           'W730':'graduate',
                           'W731':'graduate'}

    df_detail, df_outcome = simplified_outcome_detail_code(outcome_dat, detail_dat)
    
    #I'm going to drop anything with 'end_of_year_outcome' because I think it doesn't give us any information
    df_outcome = df_outcome[df_outcome.EXIT_REASON_CD != 'W99']
    df_outcome['code'] = df_outcome['EXIT_REASON_CD'].map(APScode_to_standard)
    df_outcome.drop(['EXIT_REASON_CD'], axis = 1, inplace = True)
    df_detail['d'] = df_detail.academic_year.astype(int)
    df_detail['Exit_Date'] = '6/30/'+df_detail.d.astype(str)
    df_detail.drop(['d'], axis = 1, inplace = True)
    df_detail.grade_level[df_detail.grade_level == 'TT'] = '77'
    df_detail.grade_level[df_detail.grade_level == 'GD'] = '77'
    
    dates = df_outcome['Exit_Date'].apply(clean_date)
    df_outcome['Exit_Date']=dates
    df_outcome['date'] = df_outcome.Exit_Date
    df_detail['date'] = df_detail.Exit_Date
    
    #df = pd.concat([df_detail, df_outcome])
    df = pd.concat([df_outcome, df_detail])
    
    #I think TT and GD are continuing education codes
    #I have put grade level from outcome table as -1
   
    df = df.sort(['student_id', 'date', 'grade_level'])   
    df = df.drop_duplicates(['academic_year', 'student_id', 'code', 'school_id', 'grade_level'])
    
    neg1 = (df['grade_level']=='-1').values
    
    st_ids = df.student_id.values[neg1]
    ac_yrs = df.academic_year.values[neg1]
    new_grades = np.array([clean_grade(df_detail, st_ids[i], ac_yrs[i]) for i in range(len(st_ids))])
    
    df.grade_level[neg1] = new_grades
    df.grade_level = df.grade_level.astype(int)
    df.student_id = df.student_id.astype(int)
    df.academic_year = df.academic_year.astype(int)
    df_detail.student_id = df_detail.student_id.astype(int)
    df_detail.academic_year = df_detail.academic_year.astype(int)
    df_detail.grade_level = df_detail.grade_level.astype(int)
    
    df = df.drop_duplicates(['academic_year', 'student_id', 'code', 'school_id', 'grade_level'])
    
    g1 = (df['code']=='end_of_year').values
    st_ids = df.student_id.values[g1]
    ac_yrs = df.academic_year.values[g1]
    new_codes = np.array([clean_advance_retain(df_detail, st_ids[i], ac_yrs[i]) for i in range(len(st_ids))])
    df.code[g1] = new_codes

    df = df[df['grade_level']!=-1]
    
    g1 = (df.code == 'dropout') | (df.code == 'exit') | (df.code == 'graduate') 
    st_ids = df[g1].student_id.values
    ac_yrs = df[g1].academic_year.values
    drop_inds = list(set([clean_exit(df, st_ids[i], ac_yrs[i]) for i in range(len(st_ids))]))
    drop_inds = np.array(drop_inds)
    drop_inds = drop_inds[drop_inds>0]
 
    df.code.iloc[drop_inds]='drop'
    df = df[df.code != 'drop']
   
    #let's make sure all the columns are the right datatype
    df.student_id = df.student_id.astype(int)
    df.school_id = df.school_id.astype(int)
    df.academic_year = df.academic_year.astype(int)
    df.grade_level = df.grade_level.astype(int)
    pd.to_datetime(df.date, format='%m/%d/%Y', errors='coerce')
    df.code= df.code.astype(Enum)

    df['date'] = df['Exit_Date']
    df.drop(['Exit_Date'], axis = 1, inplace=True)
    
    
    df.to_csv(cleaned, header=True, index=False, date_format='%Y%m%d')
