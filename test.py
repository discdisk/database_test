import sqlite3
import numpy as np
import pickle


def get_blob(arr):
    return memoryview(pickle.dumps(arr))


class sql_database:
    def __init__(self, file: str='test.db'):
        self.start_conn(file)

    def switch_table(self,table):
        self.table=table+'_DATA'

    def start_conn(self, file: str):
        self.conn = sqlite3.connect(file)
        print('Opened database successfully')
        self.c = self.conn.cursor()
    def commit(self):
        self.conn.commit()

    def end_conn(self):
        self.commit()
        self.conn.close()

    def create_table(self):
        # create table
        self.c.execute(f'''CREATE TABLE IF NOT EXISTS {self.table}
            (ID INTEGER PRIMARY KEY   AUTOINCREMENT,
            FILE_NAME         TEXT    NOT NULL,
            TRANSCRIPT_WORD   BLOB    NOT NULL,
            TRANSCRIPT_CHAR   BLOB    NOT NULL,
            ORI_SOUND         BLOB    NOT NULL,
            FILTERBANK        BLOB    NOT NULL,
            MEAN_NORM_FILTER  BLOB    NOT NULL,
            FRAME_LEN         INT     NOT NULL,
            PlainOrthographicTranscription BLOB NOT NULL,
            PhoneticTranscription BLOB NOT NULL
            );''')
        print('table create successfully')

    def push_data(self, FILE_NAME: str, TRANSCRIPT_WORD: list, TRANSCRIPT_CHAR: list, ORI_SOUND: np.ndarray, FILTERBANK: np.ndarray, MEAN_NORM_FILTER: np.ndarray, FRAME_LEN: int, PlainOrthographicTranscription: list, PhoneticTranscription: list):
        # push data into database
        self.c.execute(f"INSERT INTO {self.table} (FILE_NAME,TRANSCRIPT_WORD,TRANSCRIPT_CHAR,ORI_SOUND,FILTERBANK,MEAN_NORM_FILTER, FRAME_LEN, PlainOrthographicTranscription, PhoneticTranscription) \
            VALUES (?,?,?,?,?,?,?,?,?)", (FILE_NAME, get_blob(TRANSCRIPT_WORD), get_blob(TRANSCRIPT_CHAR), get_blob(ORI_SOUND), get_blob(FILTERBANK), get_blob(MEAN_NORM_FILTER),FRAME_LEN, get_blob(PlainOrthographicTranscription), get_blob(PhoneticTranscription)))

    def get_data(self,data_class,indexes):
        cursor = self.c.execute(
            f"SELECT {','.join(data_class)} FROM {self.table} WHERE ID IN ({','.join(indexes)})")

        return list(cursor)

    @property
    def size(self):
        cursor = self.c.execute(
            f"SELECT MAX(ID) FROM {self.table}")
        return list(cursor)[0][0]



if __name__ == "__main__":
    dataset = 'SPS'

    # Data structure
    # list of dic
    #########################
    # {'ori_filename': file,
    # 'core_noncore': data_folder[:-1],
    # 'ori_sound':f'ori_sound_{save_count}{count}.npy',
    # 'fbank_feat':f'fbank_feat_{save_count}{count}.npy',
    # 'fbank_feat_mean_norm':f'fbank_feat_mean_norm_{save_count}{count}.npy',
    # 'frame_length':fbank_feat.shape[0],
    # 'PlainOrthographicTranscription':text,
    # 'PhoneticTranscription':PhoneticTranscription}

    database = sql_database('test_data.db')
    count=0
    for dataset in ['SPS_test','APS_test']:

        database.switch_table(dataset)
        database.create_table()

        data_file = np.load(
            f'/home/chenjh/Desktop/csj/making_correct_data/new_xml_logf40_meanNorm_{dataset}.npy')


        path_fmean = f'/home/chenjh/Desktop/csj/making_correct_data/new_xml_logf40_meanNorm_{dataset}/fbank_feat_mean_norm/'
        path_f     = f'/home/chenjh/Desktop/csj/making_correct_data/new_xml_logf40_meanNorm_{dataset}/fbank_feat/'
        path_ori   = f'/home/chenjh/Desktop/csj/making_correct_data/new_xml_logf40_meanNorm_{dataset}/ori_sound/'

        for data in data_file:
            count += 1
            FILE_NAME                      = data['ori_filename']
            TRANSCRIPT_WORD                = data['output_word']
            TRANSCRIPT_CHAR                = data['output_char']
            ORI_SOUND                      = np.load(path_ori + data['ori_sound'])
            FILTERBANK                     = np.load(path_f + data['fbank_feat'])
            MEAN_NORM_FILTER               = np.load(path_fmean + data['fbank_feat_mean_norm'])
            FRAME_LEN                      = data['frame_length']
            PlainOrthographicTranscription = data['PlainOrthographicTranscription']
            PhoneticTranscription          = data['PhoneticTranscription']

            database.push_data(FILE_NAME, TRANSCRIPT_WORD, TRANSCRIPT_CHAR, ORI_SOUND, FILTERBANK, MEAN_NORM_FILTER, FRAME_LEN, PlainOrthographicTranscription, PhoneticTranscription)
            if count%1000==0:
                print(dataset,count)
                database.commit()

    # database.get_data()
    database.end_conn()
