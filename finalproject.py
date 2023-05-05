import random
import time
import threading
import multiprocessing as mp
import cProfile
import os
import dispy
from multiprocessing import Process, Value, Array

mergeFileOne = "/mnt/usb1/merge1.txt"
mergeFileTwo = "/mnt/usb1/merge2.txt"

def sort_set(arr):
   n = len(arr)
   swapped = False
   for i in range(n - 1):
      for j in range(0, n - i - 1):
          if arr[j] > arr[j + 1]:
              swapped = True
              temp = arr[j]
              arr[j] = arr[j + 1]
              arr[j + 1] = temp

      if not swapped:
          return
          
def dispySort(file_location):
    print("Hello Test")
    import time, socket
    file_location = "/mnt" + file_location
    with open(file_location, 'r') as f:
        # Read the file content and split it into lines
        lines = f.read().splitlines()

    # Convert each line to an integer and store it in a list
    list = [int(x) for x in lines]

    # Print the list of numbers

    sort_set(list)
    new_file_location = file_location[:-4]+"F.txt"
    with open(new_file_location, 'w') as f:
        # Write each integer in the list followed by a newline character
        for num in list:
            f.write(str(num) + '\n')
    print(new_file_location)
    return new_file_location[4:]

def files_split(input_file_name):
    import os
    #list of file names to return
    file_name_list = []
    # set the section size in bytes
    section_size = 100000
    # open the input file for reading in binary mode

    # get the current directory
    current_dir = "/mnt/usb1"

    # iterate through the files in the directory
    for filename in os.listdir(current_dir):
        # check if the file starts with "output"
        if filename.startswith("output"):
            # construct the full path to the file
            file_path = os.path.join(current_dir, filename)

            # delete the file
            os.remove(file_path)

            # print a message to indicate which file was deleted
            print(f"Deleted file: {file_path}")
    with open(input_file_name, "rb") as infile:
        # initialize the section count and the position
        section_count = 0
        position = 0

        infile.seek(position + section_size)
        num_added = 0
        while (infile.read(1) != b"\n"):
            # print(infile.read(1))
            num_added += 1
            infile.seek(position + section_size + num_added)
        infile.seek(0)
        data = infile.read(section_size + num_added)
        # continue reading until the end of the file is reached
        file_name_list = []
        while data:
            # print("========================")
            # open the output file for writing in binary mode
            with open(current_dir+f"/output{section_count}.txt", "wb") as outfile:
                file_name_list.append(current_dir+f"/output{section_count}.txt")
                print("Creating: " + current_dir+f"/output{section_count}.txt")
                # write the current section of data to the output file
                outfile.write(data)
            # increment the section count and move the file pointer
            section_count += 1

            position += (section_size + num_added + 1)

            infile.seek(position)

            num_added = 0

            while (infile.read(1) != b"\n" and infile.read(1) != b""):
                # print(infile.read(1))
                num_added += 1
                infile.seek(position + section_size + num_added)
            infile.seek(position)
            data = infile.read(section_size + num_added)
        return file_name_list
        
def merge(fileone, filetwo, filethree):
    import sys
    totalsize = 0
    finished = False
    line2cache = -1
    with open(fileone, 'r') as f1, open(filetwo, 'r') as f2, open(filethree, "w") as f3:
        while(True):
            line1 = f1.readline()
            totalsize += 1
            if not line1:
                line1 = -1

            while(True):
                if(line2cache != -1):
                    line2 = line2cache
                else:
                    line2 = f2.readline()

                if line1 == -1 and not line2:
                    finished = True
                    break
                if not line2 or (int(line1) < int(line2) and line1 != -1):
                    line2cache = line2
                    break
                line2cache=-1
                f3.write(line2)
                totalsize+=1

            if finished:
                break
            f3.write(str(line1))

if __name__ == "__main__":
    print("Started")
    file_name_list = files_split("/mnt/usb1/milliontest.txt")
    print("Split")
    cluster = dispy.JobCluster(dispySort, nodes=['192.168.0.*'], host=['192.168.0.1'], depends=[sort_set])
    jobs = []
    mergeTimeStart = time.time()
    for n in file_name_list:
        job = cluster.submit(n)
        jobs.append(job)
    print("Jobs Assigned")
    with open(mergeFileOne, 'w') as file:
        pass
    with open(mergeFileTwo, 'w') as file:
        pass


    f1=mergeFileOne
    f2=mergeFileTwo
    finallocation = ""
    while(True):
        job1 = jobs.pop(0)
        inputfile = job1()
        print("Merging...")
        size = merge(inputfile, f1, f2)
        if(size == 1000000):
            break
        temp = f1
        finallocation = f2
        f1 = f2
        f2 = temp
    print("Final Location: " + finallocation)
