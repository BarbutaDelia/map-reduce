import mpi4py.MPI
from string import digits
from mpi4py import MPI
import os
import string
import re
import random

# the name of the directory containing the input files
inputDirectory = 'files'
intermediaryDirectory = 'intermediary'
outputDirectory = 'output'
finalFileDirectory = 'final'

# tag definitions
FILE_NAME = 0
END_OF_FILES = 1
END_OF_MAPPING = 2
START_REDUCING = 3
END_OF_REDUCING = 4
CREATE_FINAL_FILE = 5

if __name__ == "__main__":

    comm = MPI.COMM_WORLD
    nrOfProcesses = comm.Get_size()
    myRank = comm.Get_rank()
    status = MPI.Status()

    cnt = 0

    fileNames = []

    # creating a list with the names of the files
    if myRank == 0:
        for filename in os.listdir(inputDirectory):
            f = os.path.join(inputDirectory, filename)

            # eliminate hidden files that macOS creates -_-
            if os.path.isfile(f) and not filename.startswith('.'):
                fileNames.append(filename)

        # distributing the files to the workers
        while cnt != len(fileNames):
            for i in range(1, nrOfProcesses):
                comm.send(fileNames[cnt], dest=i, tag=FILE_NAME)
                cnt += 1
                if cnt == len(fileNames):
                    break

        for i in range(1, nrOfProcesses):
            comm.send("end", dest=i, tag=END_OF_FILES)
            process = comm.recv(source=MPI.ANY_SOURCE, tag=END_OF_MAPPING, status=status)

        # each worker process will handle a list of files which have the first letter in a certain range
        # the master process sends the list of letters that every process will handle (from the intermediary dir)
        alphabetLetters = list(string.ascii_lowercase)

        # the list of letters is shuffled so that the work is spread equally
        random.shuffle(alphabetLetters)
        chunkSize = int(len(alphabetLetters) / (nrOfProcesses - 1))
        chunks = [alphabetLetters[x:x + chunkSize] for x in range(0, (nrOfProcesses - 1) * chunkSize, chunkSize)]
        for i in range((nrOfProcesses - 1) * chunkSize, len(alphabetLetters)):
            chunks[len(chunks) - 1].append(alphabetLetters[i])

        for i in range(1, nrOfProcesses):
            comm.send(chunks[i - 1], dest=i, tag=START_REDUCING)

        for i in range(1, nrOfProcesses):
            process = comm.recv(source=MPI.ANY_SOURCE, tag=END_OF_REDUCING, status=status)
            print("Worker finished reducing!")

        # announce worker 1 to gather all words in one file
        comm.send("createFinalFile", dest=1, tag=CREATE_FINAL_FILE)
        print("Begin creation of final file")

    ok = True
    if myRank != 0:
        while ok:
            msgFromMaster = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)

            # checking to see if there are more filesNames which need to be received
            tag = status.Get_tag()
            if tag == FILE_NAME:
                fileToHandle = msgFromMaster
                print("I am process " + str(myRank) + " and I received " + str(fileToHandle))
                pairs = []

                # reading the words from the file
                path = os.path.join(inputDirectory, fileToHandle)
                f = open(path, "r", errors="ignore")  # , encoding = "ISO-8859-1")
                text = f.read()
                text = text.lower()
                f.close()

                # replacing the punctuation marks & multiple whitespaces & eol
                text = re.sub('\W+\s*', ' ', text).replace("\n", " ")
                text = text.translate(str.maketrans('', '', digits))
                text = re.sub(' +', ' ', text)

                processDirectory = str(myRank)

                # creating a list of words
                text = text.split(" ")
                for word in text:

                    # create a directory for each process
                    path = os.path.join(intermediaryDirectory, processDirectory)
                    exists = os.path.exists(path)
                    if not exists:
                        os.mkdir(path)

                    if word != "":
                        # create a file for each word and for each appearance,
                        # write the name of the file where the word was found
                        f = open(os.path.join(path, word), "a")
                        f.write(msgFromMaster + " ")
                        f.close()

            # when a process finishes creating the intermediary files, sends a message to the master
            elif tag == END_OF_FILES:
                print("I finished mapping! " + str(myRank))
                comm.send(myRank, dest=0, tag=END_OF_MAPPING)

            elif tag == START_REDUCING:
                # the last message contains the list of letters
                print("I started reducing! " + str(myRank))
                listOfLetters = msgFromMaster

                # each file, from each dir from /intermediary is processed by the worker to which it was designated
                for path, subdirs, files in os.walk(intermediaryDirectory):
                    for name in files:
                        if name[0] in listOfLetters:

                            # dictionary with the no. of appearances for each word in constructed
                            # such that for every file in which the word appears, we have a pair of
                            # (fileName, noOfAppearances)
                            counts = dict()
                            file = open(os.path.join(path, name), "r")
                            text = file.read().split(" ")[:-1]
                            file.close()

                            for word in text:
                                if word in counts:
                                    counts[word] += 1
                                else:
                                    counts[word] = 1

                            f = open(os.path.join(outputDirectory, name), "a")
                            for pair in counts:
                                f.write("(" + pair + ", " + str(counts[pair]) + ") ")
                            f.close()

                comm.send(myRank, dest=0, tag=END_OF_REDUCING)

                # all workers, except worker 1, have finished all their tasks
                if myRank != 1:
                    ok = False

            elif tag == CREATE_FINAL_FILE and myRank == 1:

                # all output files are ordered alphabetically and all words, with the corresponding no. of appearances
                # are gathered in one final file
                print("I am worker " + str(myRank) + " and I am creating the final file!")
                outputFiles = os.listdir(outputDirectory)
                outputFiles.sort()

                for outputFile in outputFiles:
                    if not outputFile.startswith('.'):
                        path = os.path.join(outputDirectory, outputFile)
                        f = open(path, "r")
                        text = f.read()
                        f.close()
                        path = os.path.join(finalFileDirectory, "finalFile")
                        finalFile = open(path, "a")
                        finalFile.write(outputFile + " --- " + text + "\n")
                        finalFile.close()
                ok = False

    mpi4py.MPI.Finalize()
