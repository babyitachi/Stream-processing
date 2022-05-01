import sys
import os
import sys
from config import rds, TWEET, WORDSET

if (len(sys.argv) < 3):
    print("Use the command: python3 client.py <data_dir>")

DIR=sys.argv[1]

# Clear the output
rds.delete(WORDSET)
rds.zadd(WORDSET, {"foo": 0})
# Clear the input
rds.xtrim(TWEET, 0)

for (pth, dirs, files) in os.walk(DIR):
  for f in files:
    print(f)
    abs_file = os.path.join(pth, f)
    for line in open(abs_file, 'r'):
      # push all the lines into input stream
      rds.xadd(TWEET, {TWEET: line})
