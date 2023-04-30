

#this code is connecting to a DB for making analysis on Loan & payments data
import pandas as pd
pd.set_option('display.max_rows', None)
import datetime as dt
from dateutil.relativedelta import relativedelta
import numpy as np
from sqlalchemy import create_engine
import json


# In[3]:



#SQL server DB connection:connecting to the DB where tables are created and create file for logging errors
Server='DESKTOP-MF54Q11'
Database='AutoCheck'
Driver = 'ODBC Driver 17 for SQL Server'
data_conn = f'mssql://@{Server}/{Database}?driver={Driver}'
try:
    engine = create_engine(data_conn)
    con = engine.connect() 
    print("OPENED CONNECTION TO DB")
except Exception as e:
    print("Not able to connect to the DB")
    logging.basicConfig(filename='DB.log', format='%(asctime)s: %(message)s',level=logging.WARNING ,encoding = 'utf-8')
    logging.warning(e)
    raise e


# In[4]:


Loan_payment = pd.read_sql_query('select * from [AutoCheck].[dbo].[Loan_payment ] ;' ,con)#gettting Loan_payment
Borrower_Data = pd.read_sql_query('select * from [AutoCheck].[dbo].[Borrower_Data] ;' ,con)# getting Borrower_Data
Loan_Table = pd.read_sql_query('select * from [AutoCheck].[dbo].[Loan_table ] ;' ,con)#gettting Loan_data
Payment_Schedule = pd.read_sql_query('select * from [AutoCheck].[dbo].[Payment_Schedule ] ;' ,con)#gettting payment data


# In[6]:


df_merged = pd.merge(Borrower_Data, Loan_Table, how='inner', left_on = 'Borrower_Id' ,right_on = 'Borrower_id')


# In[8]:


df_merged2=pd.merge(df_merged, Loan_payment, how='inner', left_on = 'loan_id' ,right_on = 'loan_id_fk')


# In[10]:


df_merged2 = df_merged2.drop(['loan_id'] ,axis = 1)


# In[11]:


Payment_Schedule['payment_order'] = Payment_Schedule.groupby('loan_id').cumcount()+1


# In[12]:


Payment_Schedule['payment_order'] = Payment_Schedule['payment_order'].astype(str)


# In[13]:


Payment_Schedule['concatenated loan id'] = Payment_Schedule['loan_id'].str.cat(Payment_Schedule['payment_order'] , sep="_")


# In[39]:


Payment_Schedule


# In[15]:


df_merged2['payment_order'] = df_merged2.groupby('loan_id_fk').cumcount()+1


# In[16]:


df_merged2['payment_order'] = df_merged2['payment_order'].astype(str)


# In[17]:


df_merged2['concatenated loan id'] = df_merged2['loan_id_fk'].str.cat(df_merged2['payment_order'] , sep="_")


# In[41]:


result_df = df_merged2.merge(Payment_Schedule[['concatenated loan id' ,'Expected_payment_date' ,'Expected_payment_amount']],on='concatenated loan id')


# In[44]:


result_df['current_days_past_due'] = result_df['DatePaid']-result_df['Expected_payment_date']


# In[45]:


result_df['months'] = (result_df['current_days_past_due'].dt.total_seconds())/(2592000)


# In[46]:


result_df['months_int'] = result_df['months'].astype(float)
result_df['amoun_at_risk'] = (result_df['months_int'])*result_df['Expected_payment_amount']


# In[55]:


grouped_payment = Payment_Schedule.groupby('loan_id')


# In[64]:


result_df['grouped'] = result_df['Expected_payment_date'].groupby(result_df['loan_id_fk']).transform('last')


# In[65]:


result_df


# In[67]:


result_df.rename(columns = {'Expected_payment_date':'last_due_date', 'grouped':'last_repayment_date'}, inplace = True)


# In[71]:


final_results = result_df[['City' ,'zip_code' ,'Payment_frequency' ,'Maturity_date' ,'current_days_past_due' ,'last_due_date' ,'last_repayment_date','amoun_at_risk']]


# In[73]:


final_results.to_excel('output_name.xls')


# In[74]:


result_df['par_days'] = result_df['current_days_past_due'].groupby(result_df['loan_id_fk']).transform('sum')


# In[86]:


df_wih_par_days = result_df.drop_duplicates(subset=['loan_id_fk'])


# In[87]:


df_wih_par_days = df_wih_par_days[['loan_id_fk' , 'par_days']]


# In[88]:


df_wih_par_days







