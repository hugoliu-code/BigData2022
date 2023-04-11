import random
import time
import threading
import multiprocessing as mp
import cProfile
import os
import dispy
from multiprocessing import Process, Value, Array

originalFileName = "/Users/issue/Downloads/data1.txt"
mergeFileOne = "merge1"
mergeFileTwo = "merge2"


def sort_set(arr):
   # Rivero's sort function
   n = len(arr)
   # optimize code, so if the array is already sorted, it doesn't need
   # to go through the entire process
   swapped = False
   # Traverse through all array elements
   for i in range(n - 1):
      # range(n) also work but outer loop will
      # repeat one time more than needed.
      # Last i elements are already in place
      for j in range(0, n - i - 1):
          # traverse the array from 0 to n-i-1
          # Swap if the element found is greater
          # than the next element
          if arr[j] > arr[j + 1]:
              swapped = True
              temp = arr[j]
              arr[j] = arr[j + 1]
              arr[j + 1] = temp

      if not swapped:
          # if we haven't needed to make a single swap, we
          # can just exit the main loop.
          return
def dispySort(list):
    import time, socket
    sort_set(list)

    return list
def printOneThousand(fileName):
    output = []
    with open(fileName) as infile:
        n = 1000
        for line in infile:
            n -= 1
            output.append(int(line))
            if (n <= 0):
                break
    return output

if __name__ == '__main__':
   print("STARTED")
   open(mergeFileOne, 'w').close()
   open(mergeFileTwo, 'w').close()
   # Debugging
   startTime = time.time()
   cluster = dispy.JobCluster(dispySort, nodes=['192.168.0.*'], host=['192.168.0.1'], depends=[sort_set])

   jobs = []
   with open(originalFileName) as infile:
       n = 100000
       list = []
       for line in infile:
           list.append(int(line))
           n -= 1
           if (n <= 0):
               job = cluster.submit(list)
               jobs.append(job)
               list = []
               n = 100000



   while (True):
       # merge lists coming out of the out queue
       checkSize = 0
       key1 = []
       key1 = jobs.pop(0)

       ikey1 = 0
       file2 = open(mergeFileTwo,"a")
       with open(mergeFileOne) as infile:
           for line in infile:
               while(key1[ikey1] < len(key1) and key1[ikey1] < int(line)):
                   file2.write(key1[ikey1])
                   ikey1 += 1
                   checkSize += 1
               file2.write(int(line))
               checkSize += 1

       if (ikey1 < len(key1)):
           a_res.extend(key1[ikey1:])
           checkSize += len(key1[ikey1:])

       # debugging
       totalMerged += 1
       print("MERGED: {}/{}".format(totalMerged, 999))

       if checkSize == totalLength:
           print(printOneThousand(mergeFileTwo))
           print(mergeFileTwo)
           print("TOTAL TIME: {:.2f} Minutes".format((time.time()-startTime)/60))
           break

       # if not the final list, put it back into the queue to be merged again
       mergeFileOne = "merge2"
       mergeFileTwo = "merge1"

