import pandas_gbq
import numpy as np
import pandas as pd
import math as mt
import datetime as dt

def getFreshData(Credentials,ProjectId):
    bigquery_sql = " ".join(["SELECT id, DATE(CAST(created_at AS DATETIME)) AS created, DATE(CAST(updated_at AS DATETIME)) AS updated, status, assignee_id",
                             "FROM `xsolla_summer_school.customer_support`",
                             "WHERE status IN ('closed','solved')",
                             "ORDER BY updated_at"])
    dataframe = pandas_gbq.read_gbq(bigquery_sql,project_id=ProjectId, credentials=Credentials, dialect="standard")
    return dataframe

def workloadScoreStatuses(LeftBoard,RightBoard,CurrentNumOfTasks):
    if (LeftBoard == 0) & (CurrentNumOfTasks == 0) & (RightBoard == 0):
        score = 0
    elif (CurrentNumOfTasks >= 0) & (CurrentNumOfTasks < LeftBoard):
        score = 0
    elif (CurrentNumOfTasks >= LeftBoard) & (CurrentNumOfTasks <= RightBoard):
        score = 1
    else:
        score = 2
    return score


def workloadScoringByStatuses(Data,NumOfAllDays,NumOfIntervalDays):
    assignee_id = np.unique(Data.assignee_id)
    assignee_id = assignee_id[0]
    
    #splitting by status
    statuses = np.unique(Data.status)
    assignee_id_list = []
    status_list = []
    avg_num_of_task_per_week_list = []
    ste_list = []
    num_tasks_per_current_week_list = []
    score_for_status_list = []
    for status in statuses:
        dataframe_status = Data[(Data.status == str(status))][:]
    
        #time borders params
        curr_date = dt.datetime.strptime(str('2017-04-01'),'%Y-%m-%d')
        curr_date = curr_date.date()
        delta = dt.timedelta(days=NumOfAllDays)
        first_date = curr_date-delta
    
        #time interval params
        delta_interval = dt.timedelta(days=NumOfIntervalDays)
        first_interval = first_date+delta_interval
            
        num_of_intervals = int(NumOfAllDays/NumOfIntervalDays)
        num_tasks_per_week = []
        for i in range(0,num_of_intervals):
            interval = dataframe_status[(dataframe_status.updated >= str(first_date)) & (dataframe_status.updated <= str(first_interval))][:]
            first_date = first_date + delta_interval
            first_interval = first_interval + delta_interval
    
            if i != (num_of_intervals-1):        
                num_of_tasks = len(np.unique(interval['id']))
                num_tasks_per_week.append(num_of_tasks) #history number of tasks
            else:
                num_tasks_per_current_week = len(np.unique(interval['id'])) #currently number of tasks
        
        avg_num_of_task_per_week = round(np.median(num_tasks_per_week),2)

        #squared deviations
        x_values = []
        for num in num_tasks_per_week:
            x = round((num - avg_num_of_task_per_week)**2,2)
            x_values.append(x)

        #data sampling statistics
        x_sum = round(sum(x_values),2) #sum of squared deviations
        dispersion = round(x_sum/(num_of_intervals-1),2) #dispersion
        std = round(mt.sqrt(dispersion),2) #standart deviation for sample
        ste = round(std/mt.sqrt(num_of_intervals),2) #standart error for sample

        #confidence interval
        left_border = int(avg_num_of_task_per_week - ste)
        right_border = int(avg_num_of_task_per_week + ste)

        #workload scoring for status
        score_for_status = workloadScoreStatuses(left_border,right_border,num_tasks_per_current_week)        
        assignee_id_list.append(assignee_id)
        status_list.append(status)
        avg_num_of_task_per_week_list.append(avg_num_of_task_per_week)
        ste_list.append(ste)
        num_tasks_per_current_week_list.append(num_tasks_per_current_week)
        score_for_status_list.append(score_for_status)
        
    score_data = {"assignee_id":assignee_id_list,"status":status_list,
                  "count_last_period":num_tasks_per_current_week_list,"count_mean_calc_period":avg_num_of_task_per_week_list,"count_sem_calc_period":ste_list,
                  "score_value":score_for_status_list}
    scores = pd.DataFrame(data=score_data)
    return scores


def insertScoreResultData(InsertDataFrame,ProjectId,DatasetId,TableId,Developer):
    destination_table = f"{DatasetId}.{TableId}"
    
    if TableId == 'score_result_status':
        res_df = pd.DataFrame()
        res_df['assignee_id'] = InsertDataFrame['assignee_id'].astype('int')
        res_df['status'] = InsertDataFrame['status'].astype('str')
        res_df['count_last_period'] = InsertDataFrame['count_last_period'].astype('int')
        res_df['count_mean_calc_period'] = InsertDataFrame['count_mean_calc_period'].astype('float')
        res_df['count_sem_calc_period'] = InsertDataFrame['count_sem_calc_period'].astype('float')
        res_df['score_value'] = InsertDataFrame['score_value'].astype('int')
    else:
        res_df = pd.DataFrame()
        res_df['assignee_id'] = InsertDataFrame['assignee_id'].astype('int')
        res_df['score_value'] = InsertDataFrame['score_value'].astype('float')

    res_df['developer'] = Developer
    res_df['developer'] = res_df['developer'].astype('str')
    pandas_gbq.to_gbq(res_df, destination_table=destination_table, project_id=ProjectId, if_exists='append')    


def ResultStatus(DataFrameStat):
    test_result=[]
    assignee = DataFrameStat['assignee_id'].unique()
    for i in assignee:
        test_user = DataFrameStat[DataFrameStat.assignee_id == i][:]
        test_result.append(workloadScoringByStatuses(test_user,63,7))
    result=pd.concat(test_result)
    return result


def ResultTotal(DataFrameTot):
    total=pd.DataFrame(ResultStatus(DataFrameTot), columns=['assignee_id','score_value'])
    result=total.groupby('assignee_id', as_index=False).agg('mean')
    return result

