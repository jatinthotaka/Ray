import ray
ray.init()

import numpy as np
import pandas as pd
import time 
from uuid import uuid4
import os 


def create_df(row_count, num_col_count, cat_col_count):
    category = [[] for _ in range(cat_col_count)]
    for i in range(cat_col_count):
        category_size = np.random.randint(2,100) 
        category[i] = [str(uuid4()) for _ in range(category_size)]     
    dataset = {}
    for col in range(num_col_count):
        name = f'n{col}'
        values = np.random.normal(0, 1, row_count)
        dataset[name] = values        
    
    for col in range(cat_col_count):
        name = f'c{col}'
        cats = category[col] 
        values = np.array(np.random.choice(cats, row_count, replace=True), dtype=object)
        dataset[name] = values
        
    return pd.DataFrame(dataset)    

@ray.remote
class ListManager():
    def __init__(self):
        self._list = []
    def get(self):
        return self._list
    def append(self, value):
        self._list.append(value)
        
    def edit(self, loc, value):
        self._list[loc] = value
    def get_value(self,loc):
        return self._list[loc]
    def del_sf(self):
        if len(self._list) == 1 or len(self._list) == 0:
            self._list = []
        else:
            self._list = self._list[1:]    

        
@ray.remote
class Producer():
    def __init__(self, sub_df_count, row_count, num_col_count, cat_col_count):
        print('Producer initiated')
        #self.lst = dataqueue
        self.max_count = sub_df_count
        self.row_count, self.num_col_count, self.cat_col_count = row_count, num_col_count, cat_col_count
          
    def caller(self, dataqueue,result_list):
        print('Producer called')
        for i in range(self.max_count):
            df = create_df(self.row_count, self.num_col_count, self.cat_col_count)
            result_list.append.remote(['Producer' , np.NaN, (i+1)*self.row_count, pd.Timestamp.now(), 'feather'])
            #print(df)
            dataqueue.append.remote(df)

        return True
    
@ray.remote
class Writer():
    def __init__(self, sub_df_count):
        print('Writer initiated')
        self.max_count = sub_df_count*2000
        self.temp_df = pd.DataFrame()
        self.fn = 'output.ftr'
        
    def caller(self,dataqueue,result_list,reader_tracker):
        print('Writer called')
        count = 0
        while True:
            data_list = ray.get(dataqueue.get.remote())
            if len(data_list)>0 and ray.get(reader_tracker.get.remote())[0] == 0:
                
                print('Writer is updating. Data list size',len(data_list))
                #print(type(data_list[0]))
                self.temp_df = pd.concat([self.temp_df,data_list[0]], ignore_index= 1)
                #print(type(self.temp_df))
                #print(self.temp_df)
                assert isinstance(self.temp_df, pd.DataFrame), "Dataframe not found: writer"
                
                self.temp_df.to_feather('output.ftr')
                
                ray.get(dataqueue.del_sf.remote())  
                result_list.append.remote(['Writer' , sum(pd.util.hash_pandas_object(self.temp_df)), self.temp_df.shape[0], pd.Timestamp.now(), 'feather']) 
                count += 1
                print('W:', count)
                ray.get(reader_tracker.edit.remote(0,1))
                            
            if self.temp_df.shape[0] == self.max_count:
                break
        return

@ray.remote
class Reader():
    def __init__(self, sub_df_count):
        self.max_count = sub_df_count
        print('Reader created')
        
    def caller(self, consumer_count, tracker, data, result_list, writer_tracker):
        print('Reader called')
        data.append.remote(pd.DataFrame())
        count = 1
        while True:
            assert isinstance(ray.get(tracker.get.remote()), list), "Reader does not receive tracker list"

            #print(ray.get(tracker.get.remote()))
            if all(ray.get(tracker.get.remote())) and os.path.exists('output.ftr') and ray.get(writer_tracker.get.remote())[0]:
                try:
                    data.edit.remote(0, pd.read_feather('output.ftr'))
                except:
                    continue
                result_list.append.remote(['Reader', sum(pd.util.hash_pandas_object(ray.get(data.get.remote())[0])), ray.get(data.get.remote())[0].shape[0], pd.Timestamp.now(), 'feather'])               
                print('R',count)
                count+=1
                for i in range(consumer_count):
                    tracker.edit.remote(i,0)
                ray.get(writer_tracker.edit.remote(0,0))
            if ray.get(data.get.remote())[0].shape[0] == self.max_count * 2000:
                break
            
            #print(ray.get(data.get.remote()))
            
@ray.remote
class Consumer():
    def __init__(self, serial, sub_df_count):
        
        self.id = serial
        self.fn = 'output.ftr'
        self.max_count = sub_df_count
        print(f'Consumer{self.id} created')
        
    def caller(self,tracker, result_list, data):
        print(f'Consumer {self.id} called')
        while True:
            #print('consumer tracker',ray.get(tracker.get.remote()))            
            #print('consumer tracker',ray.get(tracker.get.remote()))
            assert isinstance(ray.get(tracker.get_value.remote(self.id)),int), "Consumer not receiving correct data format"
            if ray.get(tracker.get_value.remote(self.id)) == 0:
                df = ray.get(data.get.remote())[0]
                #print('Consumer',self.id,df.shape[0],sum(pd.util.hash_pandas_object(df)))
                result_list.append.remote([f'Consumer{self.id}' , sum(pd.util.hash_pandas_object(df)), df.shape[0], pd.Timestamp.now(), 'feather'])
                tracker.edit.remote(self.id,1)
                print('.',end = "")
                if df.shape[0] == self.max_count * 2000:
                    break                    
           
            
            
    
    def check(self):
        print('C :',self.id)
        return True



def main():        
    #Parameters
    sub_df_countq, row_countq, num_col_countq, cat_col_countq, consumer_count = 200, 2000, 7, 3, 50
    
    #Calling ray functions
    if os.path.exists('output.ftr'):
        os.remove('output.ftr')
    list_values = ListManager.remote()
    
    track_list = ListManager.remote()
    data_file = ListManager.remote()
    result_list = ListManager.remote()
    write_read_chk = ListManager.remote() #To check if the feather file is not being written and being read simulaneously
    ray.get(write_read_chk.append.remote(0))
    
    for i in range(consumer_count):
        track_list.append.remote(1)
  
    assert ray.get(track_list.get.remote()) == [1] * consumer_count, "Tracker not correctly initialized"
    
    producer = Producer.remote(sub_df_countq, row_countq, num_col_countq, cat_col_countq)
    writer = Writer.remote(sub_df_countq)
    reader = Reader.remote(sub_df_countq)
    
    consumers = [Consumer.remote(i, sub_df_countq) for i in range(consumer_count)]
    #print(ray.get([c.check.remote() for c in consumers]))
    actors = [producer.caller.remote(list_values,result_list), writer.caller.remote(list_values,result_list,write_read_chk),reader.caller.remote(consumer_count, track_list, data_file, result_list,write_read_chk)] + [c.caller.remote(track_list, result_list,data_file) for c in consumers]
    print(ray.get(actors))          
    print("Final list", ray.get(result_list.get.remote()))
    results = list(ray.get(result_list.get.remote()))
    result_df = pd.DataFrame(results,columns = ['Event','Hash_value','Row_count','Time_stamp','Format'])    
    result_df.to_csv(f'ray_{consumer_count}_{sub_df_countq}.csv')
    print('Program completed')
main()
