# Import Libraries
import numpy as numpy
import pandas as pd
pd.set_option('display.max_columns',100)

# Read data
df = pd.read_csv('./raw_data/customer.csv')
df=df.drop_duplicates()
df=df.drop(columns=df.columns[7::])
df["Last_Name"]=df["Last_Name"].str.strip("._/")
df['Phone_Number'] = df['Phone_Number'].str.replace(r'\D', '',regex=True)
df=df.fillna("")
df["Paying Customer"]=df["Paying Customer"].str.replace("N/a","")
df["Paying Customer"]=df["Paying Customer"].str.replace("No","N")
df["Paying Customer"]=df["Paying Customer"].str.replace("Yes","Y")
df["Do_Not_Contact"]=df["Do_Not_Contact"].str.replace("Yes","Y")
df["Do_Not_Contact"]=df["Do_Not_Contact"].str.replace("No","N")
df=df.drop(columns="Address")
df=df.rename(columns={"Paying Customer":"Paying_Customer"})
for i in df.index:
    if df.loc[i,"Do_Not_Contact"]=="Y" or df.loc[i,"Do_Not_Contact"]=="":
        df.drop(i,inplace=True)
    elif df.loc[i,"Phone_Number"]=="":
        df.drop(i,inplace=True)
df.reset_index(drop=True,inplace=True)
df.to_csv('./cleaned_data/customer_cleaned.csv',index=False)
