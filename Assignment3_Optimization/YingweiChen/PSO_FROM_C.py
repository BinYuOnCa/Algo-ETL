# Assignment 3
# Develop PSO Algorithm from C

import random
import numpy as np
import time

#random=np.random.uniform(low=0.0, high=1.0, size=1000000)
dim=30
groupsize=20
T=3
lifespan=60
IW=0.9
maxvelo=2.0
bond=np.array([-100,100])
DIM=np.double(dim)
pi=3.1415926535897932384626433

generation=np.zeros([dim,groupsize,3])
leader=np.zeros([dim,3])
compare=np.zeros([2,groupsize])

fes=0
rands=0
random_i=0


def function(vector,i):
    temp=vector[0:i]
    sum=temp.sum()
    return sum*sum

def evalfunction(vector):
    global fes
    result=0.0
    for i in range(0, dim):
        result=result+function(vector,i)
    fes=fes+1
    return result

def sort(vector):
    temp=0
    for i in range(0, groupsize):
        if vector[temp]>vector[i]:
            temp=i
    return temp

def makerand():
    global random
    random=np.random.uniform(low=0.0, high=1.0, size=1000000)
    
def randf():
    global random_i, rands
    if random_i>999998:
        random_i=0
    else:
        random_i=random_i+1
    rands=rands+1
    return random[random_i]

def exceedsprocess():
    global dim
    global generation
    global maxvelo
    for i in range(0, groupsize):
        absg=0
        for j in range(0, dim):
            if generation[j,i,1]+generation[j,i,0]>bond[1]:
                generation[j,i,1]=bond[1]-generation[j,i,0]
            if generation[j,i,1]+generation[j,i,0]<bond[0]:
                generation[j,i,1]=bond[0]-generation[j,i,0]
            absg=absg+generation[j,i,1]*generation[j,i,1]    
        absg=np.sqrt(absg)
        if (absg>maxvelo):
            for j in range(0,dim):
                generation[j,i,1]=maxvelo*generation[j,i,1]/absg
            
def PSOgeneration():
    global groupsize, dim, generation, IW, leader
    vector=np.empty(dim, dtype=np.double)
    for i in range(0, groupsize):
        for j in range(0, dim):
            generation[j,i,1]=IW*generation[j,i,1]+2*randf()*(generation[j,i,2]-generation[j,i,0])+2*randf()*(leader[j,1] - generation[j,i,0])
    exceedsprocess()
    
    for i in range(0, groupsize):
        for j in range(0, dim):
            generation[j,i,0]=generation[j,i,0]+generation[j,i,1]
    
    for i in range(0, groupsize):
        for j in range(0, dim):
            vector[j]=generation[j,i,0]
        compare[0,i]=evalfunction(vector)
        if compare[0,i]<compare[1,i]:
            compare[1,i]=compare[0,i]
            for j in range(0, dim):
                generation[j,i,2]=generation[j,i,0]
    B=sort(compare[1])
    leader[1,2]=leader[0,2]    

    if leader[2,2]>compare[1,B]:
        leader[2,2]=compare[1,B]
        leader[0,2]=compare[1,B]
        for j in range(0, dim):
            leader[j,1]=generation[j,B,2]
            leader[j,0]=generation[j,B,2]
    else:
        if leader[0,2]>compare[1,B]:
            leader[0,2]=compare[1,B]
            for j in range(0, dim):
                leader[j,0]=generation[j,B,2]
    #retun 0
    
def initialising():
    previous=randf()
    vector=np.empty(dim, dtype=np.double)
    F=np.empty(groupsize, dtype=np.double)
    for i in range(0, groupsize):
        for j in range(0, dim):
            generation[j,i,0]=4*previous*(1-previous)
            previous=generation[j,i,0]
            generation[j,i,0]=generation[j,i,2]=generation[j,i,0]*(bond[1]-bond[0]) + bond[0]
    
    for i in range(0, groupsize):
        for j in range(0, dim):
            vector[j]=generation[j,i,0]
        F[i]=evalfunction(vector)
    B=sort(F)
    
    for i in range(0, dim):
        leader[i,0]=generation[i,B,0]
        leader[i,1]=generation[i,B,0]
        
    leader[0,2]=leader[1,2]=leader[2,2]=F[B]
    
if __name__=='__main__':
    
    start_time=time.time()
    makerand()
    initialising()
    for i in range(0,2):
        for j in range(0, groupsize):
            compare[i,j]=1e200
    
    process_control=True
    while process_control:
        PSOgeneration()
        print(f"fes={fes} : leader[2,2]={leader[2,2]} : IW={IW}")
        IW=(0.5*(1-float(fes)/200000))+0.4
        if fes>200000:
            print(f"EFS={fes} rand={rands}")
            process_control=False
    end_time=time.time()
    run_time=end_time-start_time
    print(f"The code run totally {run_time} seconds")
                
    
    
# =============================================================================
#     np_arr=np.array([20,19,18,17,16,15,14,13,12,1,10,9,8,7,6,5,4,3,2,11])
#     result_function=function(np_arr,5)
#     result_evalfunction=evalfunction(np_arr)
#     sorted_array=sort(np_arr)
#     print(randf())
# =============================================================================





    
    