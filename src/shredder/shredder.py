import json
import sys
import datetime
import psycopg2
import numpy as np
from sqlalchemy import create_engine
from pandas.io.json import json_normalize
from log.logger import logger
try:
    import pandas as pd
except Exception as e:
    logger.exception(e)

class Shredder:
    input_dataframe = ''
    norm_dataframe = ''
    cursor = None
    
    def __init__(self, df):
        self.__enter__()
        self.input_dataframe = df
        logger.info('DATE conversion ...')
        self.input_dataframe['DATE'] = pd.to_datetime(self.input_dataframe['TIMESTAMP'].astype(str)).dt.date

    def __enter__(self):
        logger.info("Making connection with DB ...")
        try:
            with psycopg2.connect(user='user',
                                  password='password',
                                  host='postgres',
                                  database='database') as self.connection:
                self.cursor = self.connection.cursor()
                self.cursor.execute('SELECT version();')
                self.cursor.execute('DROP TABLE IF EXISTS article_performance')
                self.cursor.execute(self._get_article_performance_schema())
                self.cursor.execute('DROP TABLE IF EXISTS user_performance')
                self.cursor.execute(self._get_user_performance_schema())
                logger.info('tables created successfully')

        except (Exception, psycopg2.Error) as error:
            # print('Error while connecting to PostgreSQL', error)
            logger.exception(error)

    def _get_article_performance_schema(self):
        art_perf = "CREATE TABLE article_performance(id varchar(255)," \
                   "category varchar(255)," \
                   "DATE varchar(255)," \
                   "title varchar(255)," \
                   "article_viewed float(8)," \
                   "my_news_card_viewed float(8)," \
                   "top_news_card_viewed float(8)" \
                   ")"
        return art_perf

    def _get_user_performance_schema(self):
        user_perf = "CREATE TABLE user_performance(user_id varchar(255)," \
                    "ctr varchar(255)," \
                    "DATE varchar(255)" \
                    ")"
        return user_perf


    def __exit__(self):
        logger.info("Disconnecting DB ...")
        self.connection.close()
        logger.info("Connection closed")

    def _normalize_json_column(self, df_in):
        '''
        4 step process of dropping Nan, extracting json, drop json table
        :param df_in:
        :return:
        '''
        logger.info("### normalizing the json column ###")
        logger.info('1. json apply . . .')
        stage_1_jsonapply = df_in['ATTRIBUTES'].apply(json.loads)
        logger.info('2. json extract and rejoin . . .')
        stage_2_jsonextract = df_in.join(pd.DataFrame(stage_1_jsonapply.tolist()))
        logger.info('3. drop json column . . .')
        stage_3_drop_attr = stage_2_jsonextract.drop(['ATTRIBUTES'], axis=1)
        logger.info("### JSON column extracted ###")
        return stage_3_drop_attr



    def _insert_table(self, data, table_name):
        # creating column list for insertion
        cols = ",".join([str(i) for i in data.columns.tolist()])
        # print(cols)
        # Insert DataFrame records one by one.
        for i, row in data.iterrows():
            try:
                sql = f"INSERT INTO {table_name}({cols}) VALUES{tuple(row)}".format(table_name,cols,row)
                # print(sql)
                self.cursor.execute(sql)
                self.connection.commit()
            except Exception as e:
                # print(tuple(row))
                print(e)
                break
        
        self.cursor.execute(f"SELECT count(*) from {table_name}")
        record = self.cursor.fetchone()
        logger.info('row count: '+str(record))
        

    def _get_article_performance(self):
        '''
        article_id, date, title, category, article_viewed, my_news_card_viewed, top_news_card_viewed
        :return: True
        '''
        try:
            logger.info("Trying article perf . . .")
            filter_frame = self.input_dataframe.query("EVENT_NAME in ['top_news_card_viewed','my_news_card_viewed','article_viewed']")
            logger.info('Drop Nan from attributes . . .')
            filter_2 = filter_frame.dropna(subset=['ATTRIBUTES'])
            norm_1 = self._normalize_json_column(filter_2)
            logger.info('drop Nan in normalized column')
            clean_ids = norm_1.dropna(subset=['id'])
            clean_ids = clean_ids.reset_index(drop=True)

            aggregate = {'title': pd.Series.mode, 
                         'DATE': lambda x: x.mode().to_list(),
                         # 'category': pd.Series.mode,
                         'category':lambda x: x.mode().to_list(),
                         }
            result_1 = clean_ids.groupby(['id']).agg(aggregate)
            
            result_2 = clean_ids.groupby(['id','EVENT_NAME']).size().unstack().fillna(0)
            end_result = pd.merge(left=result_1, right=result_2, left_on='id', right_on='id')
            end_result['id'] = end_result.index
            end_result['DATE'] = [','.join(map(str, l)) for l in end_result['DATE']]
            end_result['category'] = [','.join(map(str,l)) for l in end_result['category']]
            end_result['title'] = end_result.title.str.replace('[\',}]','')
            end_result = end_result[['id','category','DATE','title','article_viewed','my_news_card_viewed','top_news_card_viewed']]
            # logger.info(end_result)
            self._insert_table(end_result,'article_performance')            
            return True
        
        except Exception as e:
            logger.exception(e)
            return False
        
    def user_perf(self):
        '''
        Required table => user_id, date, ctr(click through rate)
        ctr = number of articles viewed / number of cards viewed
        :return: True after loading the table
        '''
        try:
            logger.info("Trying User perf . . .")
            filter_frame = self.input_dataframe.query("EVENT_NAME in ['top_news_card_viewed','my_news_card_viewed','article_viewed']")
            user_group = filter_frame.groupby(['MD5(USER_ID)','EVENT_NAME']).size().unstack().fillna(0)
            result_1 = (user_group['article_viewed'] / user_group['my_news_card_viewed']+user_group['top_news_card_viewed']).to_frame('ctr')
            result_2 = filter_frame.groupby('MD5(USER_ID)').agg({'DATE': lambda x: x.mode().to_list()})
            end_result = pd.merge(left=result_1, right=result_2, left_on='MD5(USER_ID)', right_on='MD5(USER_ID)')
            end_result['user_id'] = end_result.index
            end_result['DATE'] = [','.join(map(str, l)) for l in end_result['DATE']]
            end_result = end_result[['ctr','DATE','user_id']]
            end_result['ctr'] = end_result['ctr'].replace([np.inf, -np.inf], np.nan)
            end_result['ctr'] = end_result['ctr'].fillna(0)
            self._insert_table(end_result,'user_performance')
            return True
        
        except Exception as e:
            logger.exception(e)
            return False
        
    def run(self):
        '''
         - article_performance calculation and load the table
         - user_performance calculation and load the table
        :return: None
        '''

        logger.info("getting the required info ...")

        if self._get_article_performance():
            logger.info("article performance loaded successfully!")
        else:
            logger.info("Could not upload article table, check logs")
            
        if self.user_perf():
            logger.info("user_performance table loaded successfully!")
        else:
            logger.info("Could not upload user table, check logs")

        self.__exit__()
