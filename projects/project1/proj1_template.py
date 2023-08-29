import math
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env

# ---------------------------------!!! Attention Please!!!------------------------------------
# Please add more details to the comments for each function. Clarifying the input 
# and the output format would be better. It's helpful for tutors to review your code.

# We will test your code with the comand like below:
# python3 project1.py -r hadoop hdfs_input -o hdfs_output --jobconf myjob.settings.years=20 --jobconf myjob.settings.beta=0.5 --jobconf mapreduce.job.reduces=2

# Please make sure that your code can be compiled on Hadoop before submission.
# ---------------------------------!!! Attention Please!!!------------------------------------

class proj1(MRJob):    

    # define your own mapreduce functions

    SORT_VALUES = True    

    def steps(self):
        JOBCONF = { 
            # add your configurations here
        }
        return [
            # you decide the number of steps used
        ]

if __name__ == '__main__':
    proj1.run()