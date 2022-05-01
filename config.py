import redis
rds = redis.Redis(host='localhost', port=6579, decode_responses=True)

TWEET = "tweet"
WORD_PREFIX = "w_"
WORD_BUCKETS = 2
TWEET_GRP = "tweet_grp"

WORDSET = "words"
