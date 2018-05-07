##################################################
##########INPUT VARIABLES#########################
import sys
NumPlayers = int(sys.argv[1])

#NumPlayers = 50000
pathname ='3-UnsortedKillStream' + str(NumPlayers) + '.txt' 
#pathname = '/home/ubuntu/PlayerStream/Project/simstream/3-UnsortedKillStream' + str(NumPlayers) + '.txt'
destname ='3-UnsortedKillStream' + str(NumPlayers) + '.txt' 
##################################################
#  http://boto3.readthedocs.io/en/latest/guide/s3-example-creating-buckets.html

import os
import sys
import threading
import boto3

# Let's use Amazon S3
s3 = boto3.client('s3')

bucket_name = 'steve-dota2'

class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()
    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()


# Uploads the given file using a managed uploader, which will split up large
# files automatically and upload parts in parallel.
s3.upload_file(pathname, bucket_name, destname, Callback=ProgressPercentage(destname))
