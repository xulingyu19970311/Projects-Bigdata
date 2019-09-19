from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

class count(MRJob):
  
   def mapper1(self,key,value):                                                                    
      parts =next(csv.reader(value.splitlines(),skipinitialspace=True))
      #remove the segement, skip the space in line
      ds = parts[0]
      dp = parts[1] 
      ct = parts[2]
      #make tags for ori and des list
      yield (ds,(dp,ct,"ori"))
      yield (dp,(ds,ct,"des"))
   
   def reducer1(self,key,value_list):
      a=[]
      b=[]
      for f in value_list:
        if f[2] == "ori":
           a.append((f[0],f[1]))
        else:
           b.append((f[0],f[1]))
      #we devide the data into original and estination list here
      for x in a:
         for y in b:
             if x[0]!=y[0]:
                 ct=int(x[1])*int(y[1])
                 yield (x[0],y[0]),ct
      #if the des of list1 = dep of list2, we muitiply the ct and join 2 lists into one

   def mapper2(self,key,value):
      yield (key,value)
       
   def reducer2(self,key,value):
      yield (key,sum(value))
      #get the sum of the the cts here 
             
   def steps(self):
        return[
           MRStep(mapper=self.mapper1,reducer=self.reducer1),
           MRStep(mapper=self.mapper2,reducer=self.reducer2)
        ]

if __name__ == '__main__':
    count.run()
