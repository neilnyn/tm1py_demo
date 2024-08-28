import asyncio
import nest_asyncio
nest_asyncio.apply()
from concurrent.futures import ThreadPoolExecutor
from TM1py.Services import TM1Service
from typing import List
import time

def read_tasks_file(filepath:str):
    
    with open(filepath,'r+') as t:
        content=t.readlines()
        tasks_list=list()
        process_list=list()
        for line in content:
    
            if line.find('\n')>0:
                line=line.replace('\n','')
            line_clean=''    
            for alph in line:
                if alph!='"':
                    line_clean=line_clean+alph
            line=line_clean        
            para=line.split(',')
            dic_final=list()
            for i in range(1,len(para)):
                dic=dict()
                #print(para[i])
                para[i].split('=')
                dic['Name']=para[i].split('=')[0]
                dic['Value']=para[i].split('=')[1]
                dic_final.append(dic)
            process_list.append(para[0].split('=')[1])    
            dic_para=dict()
            dic_para['Parameters']=dic_final
            tasks_list.append(dic_para)
        t.close()
    return process_list,tasks_list


async def run_process_async(tm1:TM1Service,processes:List,parameters:List,Threads:int):
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(Threads) as executor:
        #futures = [loop.rudata:'pd.DataFrame',n_in_executor(executor, transfer, tm1_src, tm1_tgr,cube_src, cube_tgr,month_leaf) for month_leaf in leaves]
        index=0
        futures=[]
        for parameter in parameters:  
            f=loop.run_in_executor(executor, run_process,tm1,processes[index],parameter)
            futures.append(f)
            index+=1
        for future in futures:
            try:
                await future
            except:
                print(future)
                continue

# Define Function
def run_process(tm1:TM1Service, process:str,parameter:'Dict'):
    tm1.processes.execute(process, parameter)



processes,tasks=read_tasks_file('C:\\TI Excecute PY\\RushTI_tasks.txt')

with TM1Service(address="192.168.0.176", port=30015, ssl=False, user="neil", password="123") as tm1:
    b=time.time()
    asyncio.run(run_process_async(tm1,processes,tasks,5))
    e=time.time()
    print('time_eclipse:',e-b)   