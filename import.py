from pymongo import MongoClient
from pymongo import WriteConcern
from pprint import pprint
import Queue
import threading
import time
import json
import os

THREADS = 6
exitFlag = 0

client = MongoClient('...')
pprint(client.admin.command("serverStatus"))

tateDb = client.get_database('tategallery')

def writeFileToCollection(subdir, file, collection):
    filePath = os.path.join(subdir, file)
    with open(filePath, 'r') as jsonFile:
        jsonDoc = json.load(jsonFile)
        result = tateDb.get_collection(collection).insert_one(jsonDoc)
        print('+', filePath, ',result=', result.inserted_id)


class myThread (threading.Thread):
    def __init__(self, threadID, name, q):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q

    def run(self):
        print "Starting " + self.name
        process_data(self.name, self.q)
        print "Exiting " + self.name


def process_data(threadName, q):
    while not exitFlag:
        queueLock.acquire()
        if not workQueue.empty():
            data = q.get()
            queueLock.release()
            writeFileToCollection(
                data['subdir'], data['file'], data['collection'])
        else:
            queueLock.release()
            time.sleep(1)


queueLock = threading.Lock()
workQueue = Queue.Queue(-1)
threads = []
for x in range(THREADS):
    thread = myThread(x, 'Thread-'+str(x), workQueue)
    thread.start()
    threads.append(thread)

queueLock.acquire()
for subdir, dirs, files in os.walk('./collection/artworks'):
    for file in [fi for fi in files if fi.endswith(".json")]:
        workQueue.put(dict(collection='artwork', subdir=subdir, file=file))

for subdir, dirs, files in os.walk('./collection/artists'):
    for file in [fi for fi in files if fi.endswith(".json")]:
        workQueue.put(dict(collection='artist', subdir=subdir, file=file))

print 'Seeded queue: %s' % (workQueue.qsize())
queueLock.release()

# Wait for queue to empty
while not workQueue.empty():
    pass

# Notify threads it's time to exit and wait for all threads to complete
exitFlag = 1
for t in threads:
    t.join()
print "Wait for all threads to complete."
