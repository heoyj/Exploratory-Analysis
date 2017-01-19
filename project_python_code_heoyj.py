import pandas as pd
import sqlite3 as sqlite
import sys
import os
import time

filepath1 = "/Users/youngjinheo/Dropbox/2016_fall/SI_618_Exploratory_Analysis/SI618_Final_Project/Data/"
filepath2 = "/Users/youngjinheo/Dropbox/2016_fall/SI_618_Exploratory_Analysis/SI618_Final_Project/Lookup_tables/"


# -------- make a list of dataset name in order to import -------- #

Period = pd.DataFrame({"Year": pd.Series([2011]*4+[2012]*12+[2013]*12+[2014]*12+[2015]*12+[2016]*9),
                       "Month": pd.Series(range(9, 13, 1)+range(1, 13, 1)*4+range(1, 10, 1))})

Record_Num = []
Col_Num = []
Col_List = []
df_list = []

def info(row):
    with open(os.path.join(filepath1,'%04d_%02d_OnTimeData.csv') % (row["Year"], row["Month"]), 'r') as f:
        df_ontime = pd.read_csv(f, header=0)
        Record_Num.append(df_ontime.shape[0])
        Col_Num.append(len(df_ontime.columns))
        Col_List.append(list(df_ontime.columns.values))
        df_list.append('%04d_%02d_OnTimeData.csv' % (row["Year"], row["Month"]))

# t0 = time.time()
P = Period.apply(info, axis=1)
# t1 = time.time()
# print('time = {:.1f}s'.format(t1-t0)) # time = 171.8s

P1 = pd.DataFrame(Record_Num)
P1.columns = ['Record_Num']

P2 = pd.DataFrame(Col_Num)
P2.columns = ['Col_Num']

P3 = pd.DataFrame(Col_List)

P4 = pd.DataFrame(df_list)
P4.columns = ['df_list']

Period = Period.join(P1)
Period = Period.join(P2)
Period = Period.join(P4)

# check the same number of columns in each file
sum(Period["Col_Num"].apply(lambda x: 0 if x == 31 else 1)) # all columns are 31

# check whether all the variables are equal in each file
all(P3.eq(P3.iloc[0, :], axis=1).all(1))

sum(Period["Record_Num"])     # 30302072 (before handling missing)

# Period.to_csv(os.path.join(filepath1,'Period.csv'), header=True, index=None)


# -------- concatenate multiple datasets -------- #

# Period = pd.read_csv(os.path.join(filepath1,'Period.csv'), header=0)

colname = ['YEAR', 'QUARTER', 'CARRIER', 'DEP_DELAY', 'DEP_DELAY_GROUP', 'DISTANCE', 'CARRIER_DELAY', 'DISTANCE_GROUP', 'CARRIER_DELAY', 'WEATHER_DELAY', 'NAS_DELAY', 'SECURITY_DELAY', 'LATE_AIRCRAFT_DELAY']

df_union = []
t0 = time.time()
df_union = [pd.read_csv(os.path.join(filepath1,filename), sep=',', header= 0, usecols = colname) for filename in Period['df_list']]
t1 = time.time()
print('time = {:.1f}s'.format(t1-t0))  # time = 38.8s

#concatenate them together
t0 = time.time()
df = pd.concat(df_union, ignore_index=True)
t1 = time.time()
print('time = {:.1f}s'.format(t1-t0))   # time = 2.7s

# df.shape   # (30302072, 12)


# -------- Data check -------- #
# 1) keep the data when delay_group >= 0
counts1 = df['DEP_DELAY_GROUP'].value_counts()

df = df[df['DEP_DELAY_GROUP'] >= 0]
# df.shape    # (12903778, 12)

# 2) check how many carriers are in this dataset
df['CARRIER'].value_counts()  # 19 carriers




# -------- add 'delay_reason' column-------- #
df_reason = df.dropna(subset = ['CARRIER_DELAY', 'WEATHER_DELAY', 'NAS_DELAY', 'SECURITY_DELAY', 'LATE_AIRCRAFT_DELAY'])
# df_reason.shape  # (5108613, 12)

Reason1 = pd.Series(df_reason['CARRIER_DELAY'].apply(lambda x : 1 if x > 0.0 else 0), name='reason1')
Reason2 = pd.Series(df_reason['WEATHER_DELAY'].apply(lambda x : 2 if x > 0.0 else 0), name='reason2')
Reason3 = pd.Series(df_reason['NAS_DELAY'].apply(lambda x : 3 if x > 0.0 else 0), name='reason3')
Reason4 = pd.Series(df_reason['SECURITY_DELAY'].apply(lambda x : 4 if x > 0.0 else 0), name='reason4')
Reason5 = pd.Series(df_reason['LATE_AIRCRAFT_DELAY'].apply(lambda x : 5 if x > 0.0 else 0), name='reason5')

R = pd.concat([Reason1, Reason2, Reason3, Reason4, Reason5], axis=1)

df_reason = pd.concat([df_reason, R], axis=1)




# -------- add 'obs_num' column-------- #
obs_num = pd.DataFrame({'obs_num' : range(1,df_reason.shape[0]+1,1)})

df_reason = df_reason.reset_index(drop=True)
df_reason = pd.concat([df_reason, obs_num], axis=1)

# df_reason.shape    # (5108613, 18)




# -------- reshape the dataset -------- #
df1 = df_reason[['YEAR', 'QUARTER', 'CARRIER', 'DEP_DELAY', 'DEP_DELAY_GROUP', 'DISTANCE', 'DISTANCE_GROUP', 'reason1', 'reason2', 'reason3', 'reason4', 'reason5', 'obs_num']]
df1 = pd.melt(df1, id_vars=['YEAR', 'QUARTER', 'CARRIER', 'DEP_DELAY', 'DEP_DELAY_GROUP', 'DISTANCE', 'DISTANCE_GROUP', 'obs_num'], var_name='delay_reason')
# df1.shape   # (25543065, 10)

df1 = df1[df1['value'] != 0]
# df1.shape   # (8814394, 10)

# sort by obs_num
df2 = df1.sort_values(['obs_num', 'YEAR', 'QUARTER', 'CARRIER', 'value'], ascending=[True, True, True, True, True])
df2 = df2.reset_index(drop=True)
del df2['delay_reason']

# melt reference : <http://pandas.pydata.org/pandas-docs/stable/reshaping.html>

# -------- generate DB -------- #
t0 = time.time()
con = None

try:
    con = sqlite.connect(os.path.join(filepath1,'OnTimePerformance.db'))

    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS OnTime")
    cur.execute("CREATE TABLE OnTime(obs_num INT, year INT, quarter INT, carrier TEXT, dep_delay INT, dep_delay_group INT, distance INT, distance_group INT, delay_reason INT)")
    cur.executemany("INSERT INTO OnTime (obs_num, year, quarter, carrier, dep_delay, dep_delay_group, distance, distance_group, delay_reason) VALUES(?,?,?,?,?,?,?,?,?)",list(df1[['obs_num', 'YEAR', 'QUARTER', 'CARRIER', 'DEP_DELAY', 'DEP_DELAY_GROUP', 'DISTANCE', 'DISTANCE_GROUP', 'value']].to_records(index=False)))
    con.commit()

except sqlite.Error, e:

    print "Error %s:" % e.args[0]
    sys.exit(1)

finally:
    if con:
        con.close()

t1 = time.time()
print('Generating DB takes time = {:.1f}s'.format(t1 - t0))


# # -------- check the data -------- #
# g = df2.groupby(['YEAR', 'QUARTER','CARRIER','value'])
# g.size()
