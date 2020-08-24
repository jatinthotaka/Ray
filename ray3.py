import ray; ray.init()
import numpy as np
import pandas as pd
import time 

def create_df(row_count, num_col_count, cat_col_count):
    dataset = {}
    for col in range(num_col_count):
        name = f'n{col}'
        values = np.random.normal(0, 1, row_count)
        dataset[name] = values        
    
    for col in range(cat_col_count):
        name = f'c{col}'
        cats = 4 #To be edited later. This variable is the factors for a given category 
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
        
@ray.remote
class Producer():
    def __init__(self, sub_df_count, row_count, num_col_count, cat_col_count):
        print('Producer initiated')
        #self.lst = dataqueue
        self.max_count = sub_df_count
        self.row_count, self.num_col_count, self.cat_col_count = row_count, num_col_count, cat_col_count
    def caller(self, dataqueue):
        print('Producer called')
        for i in range(self.max_count):
            #df = create_df(self.row_count, self.num_col_count, self.cat_col_count)
            #print(df)
            dataqueue.append.remote(i)
            print('producer',ray.get(dataqueue.get.remote()))
        return True
    
@ray.remote
class Writer():
    def __init__(self, sub_df_count):
        print('Writer initiated')
        self.max_count = sub_df_count
    
    def caller(self,dataqueue):
        print('Writer called')
        time.sleep(1)
        print('List value',ray.get(dataqueue.get.remote()))
        return (ray.get(dataqueue.get.remote()))

#Parameters
sub_df_countq, row_countq, num_col_countq, cat_col_countq = 2, 10, 2, 2      

#Calling ray functions
list_values = ListManager.remote()    
producer = Producer.remote(sub_df_countq, row_countq, num_col_countq, cat_col_countq)
writer = Writer.remote(sub_df_countq)
print(ray.get([producer.caller.remote(list_values), writer.caller.remote(list_values)]))          
