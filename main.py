import random
import time
import threading
import multiprocessing as mp
import cProfile
import os
from multiprocessing import Process, Value, Array
#  size of the list to be sorted
totalLength = 1000000

# total number of groups the list is to be split into
groupSize = 1000

# maximum number to be generated in the list to be sorted
maxSize = 500000

# how many processes to use
processCount = 5

# debugging

# time to create initial list
# time to split list
# time to sort on each thread
# time to merge

startTimeCreateList = 0
startTimeSplitList = 0



totalSorted = 0
totalMerged = 0

timeToSort = 0
timeToMerge = 0


def breakdown(inputList, splitSize):
   # takes in a list, outputs a list of size splitSize, with mostly equal sub lists
   outputList = []

   # how long will most sub lists be
   subarraySize = int(len(inputList)/splitSize)

   for i in range(splitSize):
       if i * splitSize + subarraySize < len(inputList):
           outputList.append(inputList[i * splitSize: i * splitSize + subarraySize])
       else:
           # if splitSize doesn't evenly split inputList
           outputList.append(inputList[i * splitSize:])

   return outputList


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


def sort(iqueue, oqueue, timeDebugging, timeSortQueue):
   inSortStartTime = time.time()
   while True:
       # get list from queue
       nextList = iqueue.get()
       if(nextList == "DONE"):
           timeSortQueue.put(str(os.getpid()) + ": " + str(int((time.time()-inSortStartTime)*1000)))

           break
       # sort it
       sortTimeStart = time.time()
       sort_set(nextList)

       timeDebugging.value = timeDebugging.value + time.time() - sortTimeStart
       # debugging
       global totalSorted
       totalSorted += 1
       print("SORTED ({}): {}/{}".format(threading.get_ident(), totalSorted, int(groupSize/processCount)))

       # add it to the out queue
       oqueue.put(nextList)


def generateList():
   # creates a list based on global variables
   output = []
   for i in range(totalLength):
       output.append(random.randint(0, maxSize))
   return output


def merge(iqueue, oqueue):
   # UNUSED FUNCTION (NOT WORKING ATTEMPT AT MULTIPROCESSING THE MERGING SEQUENCE) ====================================================
   while True:
       if (iqueue.empty()):
         print("BREAK")
         break
       #print(time.perf_counter())
       key1 = iqueue.get()
       if (iqueue.empty()):
         print("BREAK2")
         iqueue.put(key1)
         continue
       key2 = iqueue.get()
       ikey1 = 0
       ikey2 = 0
       a_res = []
       # x = 0


       while ikey1 < len(key1) and ikey2 < len(key2):
           # print("{} -> {} {}".format(x, ikey1, ikey2))
           # x+=1
           if key1[ikey1] > key2[ikey2]:
             a_res.append(key2[ikey2])
             ikey2 += 1
           elif key1[ikey1] < key2[ikey2]:
             a_res.append(key1[ikey1])
             ikey1 += 1
           else:
             a_res.append(key2[ikey2])
             ikey2 += 1
             a_res.append(key1[ikey1])
             ikey1 += 1


       if ((ikey1 == len(key1)) and
             (ikey2 < len(key2))):
         a_res.extend(key2[ikey2:])
       if ((ikey1 < len(key1)) and
             (ikey2 == len(key2))):
         a_res.extend(key1[ikey1:])
       # print("*" * 10)
       # print(a_res)
       if(len(a_res) == totalLength):
         oqueue.put(a_res)
         print("BREAK3")
         break
       iqueue.put(a_res)


if __name__ == '__main__':
   print("STARTED")

   # Debugging
   startTime = time.time()
   sortTime = Value('d', 0.0)


   preSortQueue = mp.Queue()
   postSortQueue = mp.Queue()
   sortTimeQueue = mp.Queue()
   sortProcesses = [mp.Process(target=sort, args=(preSortQueue, postSortQueue, sortTime, sortTimeQueue)) for i in range(processCount)]

   for process in sortProcesses:
       process.start()

   # generate list, break it into groups, and add to the queue
   startTimeCreateList = time.time()
   inputList = generateList()
   endTimeCreateList = time.time() - startTimeCreateList
   startTimeSplitList = time.time()
   messages = breakdown(inputList, groupSize)
   endTimeSplitList = time.time() - startTimeSplitList
   for message in messages:
       preSortQueue.put(message)

   for n in range(processCount):
       preSortQueue.put("DONE")

   print("CREATED MILLION LIST QUEUE")

   mergeTimeStart = time.time()

   while True:
       # merge lists coming out of the out queue

       key1 = postSortQueue.get()
       key2 = postSortQueue.get()
       ikey1 = 0
       ikey2 = 0

       # result array
       a_res = []

       while ikey1 < len(key1) and ikey2 < len(key2):
           if key1[ikey1] > key2[ikey2]:
               a_res.append(key2[ikey2])
               ikey2 += 1
           elif key1[ikey1] < key2[ikey2]:
               a_res.append(key1[ikey1])
               ikey1 += 1
           else:
               a_res.append(key2[ikey2])
               ikey2 += 1
               a_res.append(key1[ikey1])
               ikey1 += 1

       if ((ikey1 == len(key1)) and
             (ikey2 < len(key2))):
           a_res.extend(key2[ikey2:])
       if ((ikey1 < len(key1)) and
             (ikey2 == len(key2))):
           a_res.extend(key1[ikey1:])

       # debugging
       totalMerged += 1
       print("MERGED: {}/{}".format(totalMerged, groupSize-1))

       if len(a_res) == totalLength:
           # final sorted list
           print(a_res[0:1000])
           print("TOTAL TIME: {:.2f} Minutes".format((time.time()-startTime)/60))
           timeToMergeTotal = time.time() - mergeTimeStart
           break

       # if not the final list, put it back into the queue to be merged again
       postSortQueue.put(a_res)
   print("Total Create Time: {:.0f} Milliseconds".format(endTimeCreateList*1000))
   print("Total Split Time: {:.0f} Milliseconds".format(endTimeSplitList*1000))
   print("Total Sort Time: {:.0f} Milliseconds".format(sortTime.value*1000))
   for n in range(processCount):
       print("Sort time in process {} Milliseconds".format(sortTimeQueue.get()))
   print("Total Merge Total Time: {:.0f} Milliseconds".format(timeToMergeTotal*1000)) # THE MERGE IS THE BOTTLENECK, IT TAKES LONGER THAN ANY OTHER FUNCTION

   for process in sortProcesses:
       process.kill()

