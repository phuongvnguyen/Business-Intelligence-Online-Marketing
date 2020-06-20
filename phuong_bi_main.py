"""
This OOP is to do the BI Challenge
"""
from warnings import simplefilter
simplefilter(action='ignore', category=FutureWarning)
# %matplotlib inline
from google.colab import files
import pandas as pd
import numpy as np
# %reload_ext sql
import sqlite3
import seaborn as sns
import matplotlib.pyplot as plt
from plotly.offline import iplot
import plotly.express as px

pd.options.display.float_format = '{:.2f}'.format # uppress scientific notation
# Declare your Github Repository address
A_url='https://raw.githubusercontent.com/haensel-ams/recruitment_challenge/master/BI_201805/table_A_conversions.csv'
B_url='https://raw.githubusercontent.com/haensel-ams/recruitment_challenge/master/BI_201805/table_B_attribution.csv'

# The Extract class is to extract data from your Gihub Repos address
class Extract():

  def __init__(self,A_url,B_url):
    print('\033[1m'+'Please, wait! I am extracting data from your Github Repository'+'\033[0m'+'\n...')
    self.A_url=A_url
    self.table_A_conversions=self.load_data(self.A_url)
    self.B_url=B_url
    self.table_B_attribution=self.load_data(self.B_url)
    print('Data was successfully extracted!')
  
  def load_data(self,url):
    self.data=pd.read_csv(url)
    #display(self.data.head(3))
    return self.data

# The Transform class is to combine two different  extracted datasets and do the data cleansing
# Also, to know the generanl informantion about KPIs
class Transform():

  def __init__(self,extract):
    print('\033[1m'+'I am transforming the extracted data'+'\033[0m'+'\n...')
    self.table_A_conversions=extract.table_A_conversions
    self.table_B_attribution=extract.table_B_attribution
    self.joined_tabs = self.combine_tab(self.table_A_conversions, self.table_B_attribution,'Conv_ID')
    self.time_tab=self.cleaning_data(self.joined_tabs)
    # self.infor_Data=self.get_infor(self.time_tab)
    self.get_missing=self.check_missing(self.time_tab)
    self.cleaned_tab=self.time_tab.dropna()
    display(self.cleaned_tab.head(5))
    self.infor_Data=self.get_infor(self.cleaned_tab)
    self.more_infor=self.deep_infor(self.cleaned_tab)
  
  def deep_infor(self,data):
    print('Total annual revenue: %d'%data['Revenue'].sum())
    

  def combine_tab(self,tab_1,tab_2,common_col):
    print('I am combining two data into one and coverting the time format\n...')
    self.data=pd.merge(tab_1, tab_2, on=common_col, how='outer')
    # display(self.data.head(5))
    return self.data

  def cleaning_data(self,data):
    data['Conv_Date']= pd.to_datetime(data['Conv_Date']) 
    self.data=data
    print('Data was completely transformed!')
    return self.data

  def get_infor(self,data):
    print('\033[1m'+'General information:'+'\033[0m')
    self.information=data.info()
    print('\033[1m'+'Descriptive Statistics:'+'\033[0m')
    # print(data.describe())
    return self.information

  def check_missing(self,data):
    print('\033[1m'+ 'The number of missing values:'+'\033[0m')
    self.miss_data=data.isnull().sum()
    self.miss_rate=100*data.isnull().sum()/len(data)
    self.mis_infor=pd.concat([self.miss_data, self.miss_rate], axis=1).reset_index()
    self.mis_infor=self.mis_infor.rename(columns={0: 'Amounts', 1: 'Percentage'})
    # print(self.mis_infor)
    return self.miss_data

# The Load class is to load the tranformed data to the database
class  Load():

  def __init__(self,transform):
    print('\033[1m'+'I am loading the transformed data to my database'+'\033[0m'+'\n...')
    self.data=transform.cleaned_tab
    self.connect=self.connect_database()
    self.insert=self.insert_data(self.data)
    
  def connect_database(self):
    print('I am trying to connect to my SQL database\n....')
    self.connect= %sql sqlite:///phuong_database.db
    print(self.connect,'connection is success!',sep='\n')
    return self.connect

  def insert_data(self,data):
    print('I am loading the transformed data to my SQL Database\n....')
    self.check =%sql DROP TABLE IF EXISTS data
    self.insert=%sql PERSIST data
    self.list_table=%sql SELECT name FROM sqlite_master WHERE type='table'
    print(self.list_table)
    self.data=%sql SELECT * FROM data LIMIT 3
    print(self.data)
    print('Data was completely inserted into my SQL Database!')
    return self.insert 

# The EDA_Overview_KPI class is to generate a preliminary overview on the KPI
class EDA_Overview_KPI():

  def __init__(self,transform):
    print('\033[1m'+'I am doing the Explanatory Data Analysis (EDA) process for Revenue KPIs'+'\033[0m'+'\n...')
    self.data=transform.cleaned_tab[['Conv_Date','Revenue','User_ID']]
    self.by_kpi=self.group_data(self.data,'Conv_Date','Revenue','User_ID')
    # display(self.by_kpi.head(3))
    self.kpi_fig=self.plot_kpi(self.by_kpi)
    self.sum_stat=self.get_infor(self.by_kpi,'Conv_Date','Revenue','User_ID')
      
    
  def group_data(self,data,target,exp_1,exp_2):
    self.num_target=len(data[target].unique())
    print('The number of '+target+': %d'%self.num_target)
    self.data=data.groupby([target]).agg({exp_1:'sum',exp_2:'count'})
    return self.data

  def plot_kpi(self,data):
    self.name_column=self.data.columns
    plt.figure(figsize=(15, 9))
    for i,col in enumerate(self.name_column):
        plt.subplot(2,1,i+1)
        plt.plot(self.data[col],label=col)
        plt.title('The changes in of the daily '+col +' over the time period',fontweight='bold',fontsize='12')
        plt.legend()
        plt.autoscale(enable=True, axis='both',tight=True)
    plt.savefig('Overview_KPI.png')
    files.download('Overview_KPI.png')
    return self.name_column

  def get_infor(self,data,target,exp_1,exp_2):
    self.infor=display(self.data.head(8).T)
    print('\033[1m'+'Desriptive Statistics of the Daily KPIs by '+ target +'\033[0m', self.data.describe(),sep='\n')
    print('Date with the highest revenue:', self.data[exp_1].idxmax(axis = 0) )
    print('Date with the lowest revenue:', self.data[exp_1].idxmin(axis = 0) )
    print('Date with the highest number of users:', self.data[exp_2].idxmax(axis = 0) )
    print('Date with the lowest number of users:', self.data[exp_2].idxmin(axis = 0) )
    return self.infor

# The EDA_KPI_Return class is to generate a preliminary overview on the return customer
class EDA_KPI_Return():

  def __init__(self,transform):
    print('\033[1m'+'I am doing the Explanatory Data Analysis (EDA) process for User KPIs'+'\033[0m'+'\n...')
    self.data=transform.cleaned_tab[['Conv_Date','User_ID']]
    self.infor_user=self.get_infor(self.data,'User_ID')
    self.by_user=self.group_data(self.data,'User_ID','Conv_Date')
    display(self.by_user.head(8).T)
    self.user_plot=self.plot_user(self.by_user,'Conv_Date')

  def get_infor(self,data,exp):
    self.num_user=data[exp].unique()
    print('The number of users: %d'%len(self.num_user))
    return self.num_user

  def group_data(self,data,target,exp):
    self.num_target=len(data[target].unique())
    print('The number of '+target+': %d'%self.num_target)
    self.data=data.groupby([target]).agg({exp:'count'})
    # display(self.data.head(8).T)
    print('\033[1m'+'Desriptive Statistics of the Daily KPIs by '+ target +'\033[0m', self.data.describe(),sep='\n')
    return self.data

  def plot_user(self,data,exp):
    self.data=data.rename(columns={exp: 'The number of returns'})
    self.ax=self.data.plot.hist(figsize=(15, 9),bins=1500,xlim=(1,20),color='#86bf91'
                                ,title='The Frequence of return customer',grid=True)
    self.ax.set_xlabel('The number of days')
    plt.savefig('Customer_return.png')
    files.download('Customer_return.png') 
    return self.ax

# The EDA_Static_Ren class is to explore the information about the total revenue per year
class EDA_Static_Ren():
  
  def __init__(self,transform):
    print('\033[1m'+'I am  doing the EDA on Conversion'+'\033[0m'+'\n...')
    self.data=transform.cleaned_tab[['Channel','Revenue']]
    display(self.data.head(3))
    # self.infor_conver=self.get_infor(self.data)
    self.by_ChanelRen=self.group_data(self.data,'Channel')
    self.pie_ChanelRen=self.plot_pie(self.by_ChanelRen,'Revenue')

  def plot_pie(self,data,target):
    self.data=data
    self.data['Total Conver'] = self.data.sum(axis=1)
    self.data['Total Top Five'] = self.data[['A','G','H','I','B']].sum(axis=1)
    self.data['The Rest'] = self.data['Total Conver']-self.data['Total Top Five']
    self.ax=self.data[['A','G','H','I','B','The Rest']].T.plot.pie(y=target,figsize=(12, 7),autopct='%1.1f%%',)
    plt.savefig('channel_Static_Ren.jpg')
    files.download('channel_Static_Ren.jpg') 
    return self.data
    

  def get_infor(self,data):
    self.conver_uni=self.data.User_ID.unique()
    print('The number of conversions: %d'%len(self.conver_uni))
    return self.conver_uni

  def group_data(self,data,target):
    print('I am grouping data by '+ target + '\n...')
    self.data=data.groupby([target]).agg({'Revenue':'sum'})
    self.data=self.data.T
    display(self.data)
    print('I am done! ')
    return self.data    

# The EDA_Static_User class is to generate information about the total annual number of visits
class EDA_Static_User():
  
  def __init__(self,transform):
    print('\033[1m'+'I am  doing the EDA on Conversion'+'\033[0m'+'\n...')
    self.data=transform.cleaned_tab[['Channel','User_ID']] #'Conv_Date',
    display(self.data.head(3))
    # self.infor_conver=self.get_infor(self.data)
    self.by_ChanelConv=self.group_data(self.data,'Channel')
    self.pie_channelConv=self.plot_pie(self.by_ChanelConv,'User_ID')

  def plot_pie(self,data,target):
    self.data=data
    self.data['Total Conver'] = self.data.sum(axis=1)
    self.data['Total Top Five'] = self.data[['A','G','H','I','B']].sum(axis=1)
    self.data['The Rest'] = self.data['Total Conver']-self.data['Total Top Five']
    self.ax=self.data[['A','G','H','I','B','The Rest']].T.plot.pie(y=target,figsize=(12, 7),autopct='%1.1f%%',)
    plt.savefig('channel_Static_User.jpg')
    files.download('channel_Static_User.jpg') 
    return self.data
    

  def get_infor(self,data):
    self.conver_uni=self.data.User_ID.unique()
    print('The number of conversions: %d'%len(self.conver_uni))
    return self.conver_uni

  def group_data(self,data,target):
    print('I am grouping data by '+ target + '\n...')
    self.data=data.groupby([target]).agg({'User_ID':'count'})
    self.data=self.data.T
    display(self.data)
    print('I am done! ')
    return self.data    

# The EDA_Static_Conversion is to generate the information about the total annual number of conversion
class EDA_Static_Conversion():
  
  def __init__(self,transform):
    print('\033[1m'+'I am  doing the EDA on Conversion'+'\033[0m'+'\n...')
    self.data=transform.cleaned_tab[['Channel','Conv_ID','IHC_Conv']] #'Conv_Date',
    display(self.data.head(3))
    self.infor_conver=self.get_infor(self.data)
    self.by_ChanelConv=self.group_data(self.data,'Channel','Conv_ID')
    self.pie_channelConv=self.plot_pie(self.by_ChanelConv,'Conv_ID')


  def get_infor(self,data):
    self.conver_uni=self.data.Conv_ID.unique()
    print('The number of conversions: %d'%len(self.conver_uni))
    return self.conver_uni

  def group_data(self,data,target,exp):
    print('I am grouping data by '+ target + '\n...')
    if data[exp].dtype=='object':
      self.data=data.groupby([target]).agg({exp:'count'})
    else:
      self.data=data.groupby([target]).agg({exp:'sum'})
    self.data=self.data.T
    display(self.data)
    print('I am done! ')
    return self.data    

  def plot_pie(self,data,target):
    self.data=data
    self.data['Total Conver'] = self.data.sum(axis=1)
    self.data['Total Top Five'] = self.data[['A','G','H','I','B']].sum(axis=1)
    self.data['The Rest'] = self.data['Total Conver']-self.data['Total Top Five']
    self.ax=self.data[['A','G','H','I','B','The Rest']].T.plot.pie(y=target,figsize=(12, 7),autopct='%1.1f%%',)
    plt.savefig('channel_Conver.png')
    files.download('channel_Conver.png') 
    return self.data

# The EDA_Channel_Revenue class is to analyze the impacts of the online marketing channels on 
# the daily Revenue
class EDA_Channel_Revenue():

  def __init__(self,transform):
    print('\033[1m'+'I am analyzing the influences of the online marketing channels on the daily revenue'+'\033[0m'+'\n...')
    self.data=transform.cleaned_tab[['Conv_Date','Channel','Revenue']]
    self.by_DateChannel=self.group_data(self.data,'Conv_Date','Channel')
    self.unstaked_data=self.unstack_data(self.by_DateChannel,'Revenue','bar')
    self.plotted_data=self.plot_data(self.unstaked_data)   
    self.exported_data=self.export_data(self.unstaked_data,'channel_revenue')

  def group_data(self,data,target_1,target_2):
    print('I am grouping data by '+ target_1 +' and '+ target_2 + '\n...')
    self.data=data.groupby([target_1,target_2])#.agg({exp:'count'})
    print('I am done! ')
    return self.data    

  def unstack_data(self,data,exp,kind):
    print('I am unstacking data \n...')
    data=data.sum()[exp].unstack(level=-1)
    self.data=data
    display(self.data.head(3))
    print('Data were unstacked completely\n...')
    return self.data

  def plot_data(self,data):
    self.data=data
    print('I am visualizing the contribution of Top 5 Channels to the Daily Revenue\n...')
    self.data['The Total'] = self.data.sum(axis=1)
    self.data['The Rest']= self.data['The Total']-self.data[['A','G','H','I','B']].sum(axis=1)
    self.xlim=('2017-03-01','2018-03-24')
    self.ax =self.data[['A','G','H','I','B','The Rest']].plot.area(xlim=self.xlim, figsize=(12,8))
    self.ax.set_xlabel('Date')
    self.ax.set_ylabel('Revenue')
    print(self.data['The Rest'].describe())
    plt.savefig('channel_ren.png')
    files.download('channel_ren.png') 
    return self.data
    
  def export_data(self,data,title):
    print('I am exporting data to the excel and csv files\n...')
    data.to_excel(title+'.xlsx')
    self.excel=files.download(title+'.xlsx')
    data.to_csv(title+'.csv')
    self.csv=files.download(title+'.csv')
    return self.excel

# The EDA_Channel_User class is to analyze the impacts of the online marketing channels on 
# the daily number of users
class EDA_Channel_User():

  def __init__(self,transform):
    print('\033[1m'+'I am analyzing the influences of the online marketing channels on the daily number of users'+'\033[0m'+'\n...')
    self.data=transform.cleaned_tab[['Conv_Date','Channel','User_ID']]
    self.by_DateUser=self.group_data(self.data,'Conv_Date','Channel','User_ID')
    self.unstaked_data=self.unstack_data(self.by_DateUser,'User_ID','bar')
    #display(self.unstaked_data.head(3))
    self.plotted_data=self.plot_data(self.unstaked_data)   
    # self.exported_data=self.export_data(self.unstaked_data,'channel_num_user')

  def group_data(self,data,target_1,target_2,exp):
    print('I am grouping data by '+ target_1 +' and '+ target_2 + '\n...')
    self.data=data.groupby([target_1,target_2])#.agg({exp:'count'})
    print('I am done! ')
    return self.data    

  def unstack_data(self,data,exp,kind):
    print('I am unstacking data \n...')
    data=data.count()[exp].unstack(level=-1)
    self.data=data
    print('Data were unstacked completely\n...')
    return self.data

  def plot_data(self,data):
    self.data=data
    print('I am visualizing the contribution of Top 5 Channels to the Daily Number of Users\n...')
    self.data['The Total'] = self.data.sum(axis=1)
    self.data['The Rest'] = self.data['The Total'] - self.data[['A','G','H','I','B']].sum(axis=1)
    self.xlim=('2017-03-01','2018-03-24')
    self.ax =self.data[['A','G','H','I','B','The Rest']].plot.area(xlim=self.xlim, figsize=(12,8))
    self.ax.set_xlabel('Date')
    self.ax.set_ylabel('The number of Users')
    plt.savefig('channel_user.png')
    files.download('channel_user.png') 
    return self.data
    
  def export_data(self,data,title):
    print('I am exporting data to the excel and csv files\n...')
    data.to_excel(title+'.xlsx')
    self.excel=files.download(title+'.xlsx')
    data.to_csv(title+'.csv')
    self.csv=files.download(title+'.csv')
    return self.excel

# The EDA_channel_IHC class is to generate the changes in the daily IHC of Channels
class EDA_channel_IHC():

  def __init__(self,transform):
    print('\033[1m'+'I am  doing the EDA on Conversion'+'\033[0m'+'\n...')
    self.data=transform.cleaned_tab[['Conv_Date','Channel','IHC_Conv']] #'Conv_Date',
    self.by_TimeChannel=self.group_data(self.data,'Conv_Date','Channel','IHC_Conv')
    self.unstacked_data=self.unstack_data(self.by_TimeChannel,'IHC_Conv')
    self.change_plot=self.plot_data(self.unstacked_data)

  def plot_data(self,data):
    self.data=data
    # self.data['The Rest'] = self.data.sum(axis=1)
    self.xlim=('2017-03-01','2018-03-24')
    self.ylim=('0','550')
    self.ax =self.data[['A','G','H','I','B']].plot.line(xlim=self.xlim,figsize=(12,8))
    self.ax.set_xlabel('Date')
    self.ax.set_ylabel('IHC_Conv')
    plt.savefig('channel_IHC.png')
    files.download('channel_IHC.png') 
    return self.data

  def group_data(self,data,target_1,target_2,exp):
    print('I am grouping data by '+ target_1 +' and '+ target_2 + '\n...')
    self.data=data.groupby([target_1,target_2])#.agg({exp:'sum'})
    print('I am done! ')
    return self.data    

  def unstack_data(self,data,exp):
    print('I am unstacking data \n...')
    data=data.sum()[exp].unstack(level=-1)
    self.data=data
    print('Data were unstacked completely\n...')
    return self.data


class main():
  Extract=Extract(A_url,B_url)
  Transform=Transform(Extract)
  Load=Load(Transform)
  EDA_Overview_KPI=EDA_Overview_KPI(Transform)
  EDA_Static_Ren=EDA_Static_Ren(Transform)
  EDA_KPI_Return=EDA_KPI_Return(Transform)
  EDA_Static_User=EDA_Static_User(Transform)
  EDA_Static_Conversion=EDA_Static_Conversion(Transform)
  EDA_Channel_Revenue=EDA_Channel_Revenue(Transform)
  EDA_Channel_User=EDA_Channel_User(Transform)
  EDA_channel_IHC=EDA_channel_IHC(Transform)
 

if __name__=='__main__':
  main()