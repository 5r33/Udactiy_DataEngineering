import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    #process - input 1json file and insert the data into songs and artists table
    #input - 
        cur - reference to connected database
        filepath - path to the file to be processes
    #Output
        populate song data to songs table and artist data to artists table
    """
    # open song file
    df = pd.DataFrame(pd.read_json( filepath,
                                    lines=True,
                                    orient='columns'))

    # insert song record
    song_data = (   df.values[0][7],
                    df.values[0][8],
                    df.values[0][0],
                    df.values[0][9],
                    df.values[0][5])
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = ( df.values[0][0],
                    df.values[0][4],
                    df.values[0][2],
                    df.values[0][1],
                    df.values[0][3])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    #process - input 1json file and insert the data into songs and artists table
    #input - 
        cur - reference to connected database
        filepath - path to the file to be processes
    #Output
        populate time data to time table , user data to users table and song_play data in songplay table
    """    
    # open log file
    df = pd.DataFrame(pd.read_json( filepath,
                                    lines=True,
                                    orient='columns'))
    # creating a copy of df as df gets modified
    df_orig=df

    # filter by NextSong action
    df = df[df['page']=='NextSong'] 

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms') 
    
    # insert time data records
    time_data = list(zip(   t.dt.strftime('%Y-%m-%d %I:%M:%S'),
                            t.dt.hour,
                            t.dt.day,
                            t.dt.week,
                            t.dt.month,
                            t.dt.year,
                            t.dt.weekday))
    column_labels = (       'start_time',
                            'hour',
                            'day',
                            'week',
                            'month',
                            'year',
                            'weekday')
    time_df = pd.DataFrame( time_data,
                            columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_data = df_orig.get([   'userId',
                                'firstName',
                                'lastName',
                                'gender',
                                'level'])
    # adjust column names
    user_data.columns = [   'user_id',
                            'first_name',
                            'last_name',
                            'gender',
                            'level']
    # remove rows with no user_id
    #print(user_data)
    user_data=user_data[user_data['user_id'].astype(str)!= '']
    #user_data=user_data[user_data['user_id'].notnull()]
    # remove duplicates
    user_data.drop_duplicates(subset ="user_id", keep='first',inplace = True) 
    
    user_df = user_data

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        start_time = pd.to_datetime(row.ts, unit='ms').strftime('%Y-%m-%d %I:%M:%S')
        songplay_data = (   start_time,
                            row.userId,
                            row.level,
                            str(songid),
                            str(artistid),
                            row.sessionId,
                            row.location,
                            row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Traverse whole input data directory strcture,
    #input
        cur - reference to connected db.
        conn -parameters (host, dbname, user, password) to connect the db.
        filepath -path to file to be processed(data/song_data or data/log_data).
        func - function to be called (process_song_data or process_log_data)
    #Output:
        console print of the data processing.
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
        #Connect to DB
        #call process_data to walk throughvall the input data (data/song_data and data/log_data).
        Output:
        #Data loaded to the tables
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    #Process the song data and populate the dimension tables
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    
    #Process the Log data and populate the Fact/dimension tables
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()