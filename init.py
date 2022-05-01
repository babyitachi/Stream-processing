from config import rds, TWEET, WORD_PREFIX, WORD_BUCKETS, TWEET_GRP
## Configure consumer groups and workers
from tasks import running
from celery import group

def setup_stream(stream_name: str):
  garbage = {"a": 1}
  # Create the stream by adding garbage values
  id = rds.xadd(stream_name, garbage)
  rds.xdel(stream_name, id)

rds.flushall()
setup_stream(TWEET)

for i in range(WORD_BUCKETS):
  stream_name = f"{WORD_PREFIX}{i}"
  setup_stream(stream_name)
  rds.xgroup_create(stream_name,stream_name+stream_name,0)

rds.xgroup_create(TWEET,TWEET_GRP,0)

g=group(running.s())
g.apply_async()