import sqlite3
import numpy as np 
import pickle

class sql_database:
    def __init__(self,file:str='test.db'):
        self.start_conn(file)

    def start_conn(self,file:str):
        self.conn = sqlite3.connect(file)
        print('Opened database successfully')
        self.c = self.conn.cursor()

    def end_conn(self):
        self.conn.commit()
        self.conn.close()

    def create_table(self):
        ## create table
        self.c.execute('''CREATE TABLE AUDIO_DATA
            (ID INTEGER PRIMARY KEY   AUTOINCREMENT,
            FILE_NAME         TEXT    NOT NULL,
            TRANSCRIPT        BLOB    NOT NULL,
            ORI_SOUND         BLOB    NOT NULL,
            FILTERBANK        BLOB    NOT NULL,
            MEAN_NORM_FILTER  BLOB    NOT NULL );''')
        print('table create successfully')

    def push_data(self, FILE_NAME:str,TRANSCRIPT:list,ORI_SOUND:np.ndarray,FILTERBANK:np.ndarray,MEAN_NORM_FILTER:np.ndarray):
        ## push data into database
        self.c.execute("INSERT INTO AUDIO_DATA (FILE_NAME,TRANSCRIPT,ORI_SOUND,FILTERBANK,MEAN_NORM_FILTER) \
            VALUES (?, ?, ?,?,?)",(FILE_NAME,TRANSCRIPT,memoryview(ORI_SOUND.dumps()),memoryview(FILTERBANK.dumps()),memoryview(MEAN_NORM_FILTER.dumps())))

    def get_data(self):
        ## get data
        cursor = self.c.execute("SELECT ID,FILE_NAME,TRANSCRIPT,ORI_SOUND,FILTERBANK,MEAN_NORM_FILTER from AUDIO_DATA")
        for row in cursor:
            print ("ID = ", row[0])
            print ("FILE_NAME = ", row[1])
            print ("TRANSCRIPT = ", row[2])
            print ("ORI_SOUND = ", pickle.loads(row[3]))
            print ("FILTERBANK = ", pickle.loads(row[4]))
            print ("MEAN_NORM_FILTER = ", pickle.loads(row[5]))

if __name__ == "__main__":
    

    data=np.arange(12)
    print(type(data))
    database=sql_database()
    database.push_data('asn','dsfd',data,data,data)
    database.get_data()
    database.end_conn()
    