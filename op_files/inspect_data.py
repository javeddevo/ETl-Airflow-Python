from reading_data import read_data

def inspect(path):
    df=read_data(path)
    print(df)
    columns=df.columns
    print(f"total no of columns:{columns}")
    print(f"total no of rows and columns is :{df.shape}")



