import pandas as pd
from re import search

def stratify(path_to_csv):
    df_audio = pd.read_csv(path_to_csv, dtype={'AudioMothID': 'str', 'StartDateTime':'str', 'Artist':'str'})
    export_name = "stratified.csv"

    # filter to clips that are at least 60 seconds long and did not have errors
    df_audio = df_audio[(df_audio['Duration'] >= 60) & 
                        (df_audio['Error'].isnull()) &
                        (df_audio['FileSize'] >= 46080000)]

    # if startdatetime is NA get it from comments
    df_audio['backupTime'] = df_audio['Comment'].apply(lambda x : search(r'\d{2}:\d{2}:\d{2} \d{2}/\d{2}/\d{4}', x).group())
    df_audio['StartDateTime'].fillna(df_audio['backupTime'], inplace=True)
    
    # create a new column called hour, gets hour from the start date
    df_audio['hour'] = pd.to_datetime(df_audio['StartDateTime']).dt.hour

    # for each unique audiomoth, remove those that are smaller than 24 hours
    for audiomoth in df_audio["AudioMothCode"].unique():
        time_df = df_audio[df_audio['AudioMothCode']==audiomoth]
        if not (time_df['hour'].unique().size == 24):
            df_audio = df_audio[df_audio['AudioMothCode'] != audiomoth]

    # sample one from each hour
    temp_df = df_audio.groupby(['AudioMothCode', 'hour']).apply(lambda x: x.sample(1))
    
    #export to csv
    temp_df.to_csv(export_name)
    
    #returns if file was made sucessfully
    return not pd.read_csv(export_name).empty
