from celery import Celery, chord, group,chain
from config import rds, TWEET, WORD_PREFIX, WORDSET, TWEET_GRP
app= Celery('tasks',backend="redis://localhost",broker='pyamqp://guest@localhost//')

@app.task
def printop(op):
	return

@app.task
def running(init=True,retry=5,tweetpend=0,wordpend=0):
	pend = rds.xpending(TWEET,TWEET_GRP)['pending']
	pendwords=rds.xpending(f"{WORD_PREFIX}{0}",f"{WORD_PREFIX}{0}{WORD_PREFIX}{0}")['pending']+rds.xpending(f"{WORD_PREFIX}{1}",f"{WORD_PREFIX}{1}{WORD_PREFIX}{1}")['pending']
	if pend==0 and init!=True and pendwords==0:
		return
	if retry==0:
		retry=5
		tweetpend=pend
		wordpend=pendwords
		if tweetpend!=0:
			chain(fetchtweet.si(0,True),running.si(False,retry,tweetpend,wordpend))(printop.s())
		elif pendwords!=0:
			chain(countwords.si(0,True),running.si(False,retry,tweetpend,wordpend))(printop.s())
		else:
			return
	else:
		if pend==tweetpend or pendwords== wordpend:
			retry=retry-1
		else:
			retry=5
		tweetpend=pend
		wordpend=pendwords
		group(fetchtweet.s(0),running.s(False,retry,tweetpend,wordpend)).apply_async()


def splittweets(tweeps,pend=False):
	try:
		k=[]
		if type(tweeps)!=list:
			k.append(tweeps)
			tweeps=k
		

		ids=[]
		text=[]
		newl=[]
		for t in tweeps:
			if t[1]['tweet']=='\n':
				newl.append(t[0])
				continue
			text.append(" ".join(t[1]['tweet'].split(",")[4:-2]))
			ids.append(t[0])
		return ids,text,newl
	except:
		return [],[],[]

@app.task
def fetchtweet(consumer,pend=False):
	try:
		tweet=[rds.xautoclaim(TWEET,TWEET_GRP,0,0,count=1000)] if pend else rds.xreadgroup(TWEET_GRP,"C"+str(consumer),{TWEET:">"},250) 
		if tweet is not None and tweet != []:
			if len(tweet[0])<=1:
				rds.xack(TWEET, TWEET_GRP, tweet[0][0][0])
				return
			tweetids,tweettexts,newl=splittweets(tweet[0][1],pend)
			for i in newl:
				rds.xack(TWEET, TWEET_GRP, i)
			if tweettexts==[] or tweettexts==[]:
				return
			j=0
			for c,tw in enumerate(tweettexts):
				gt=[]
				for word in tw.split(" "):
					k=(j%2)
					if rds.xlen('w_0') > rds.xlen('w_1'):
						k = 1
					else:
						k = 0
					word=word.lower()
					rds.xadd(f"{WORD_PREFIX}{k}",{TWEET : word})
					j=j+1
					gt.append(countwords.s(k))
				g=group(gt)
				g.apply_async()
				rds.xack(TWEET, TWEET_GRP, tweetids[c])
		else:
			return
	except:
		print("error while fetching tweet")


def getwords(wos):
	try:
		k=[]
		if type(wos)!=list:
			k.append(wos)
			wos=k
		ids=[]
		wo=[]
		for t in wos:
			wo.append(t[1]['tweet'])
			ids.append(t[0])
		return ids,wo
	except:
		print("error in getwords")
		return [],[]

@app.task
def countwords(stream,pend=False):
	try:
		word=[rds.xautoclaim(f"{WORD_PREFIX}{stream}",f"{WORD_PREFIX}{stream}{WORD_PREFIX}{stream}",0,0,count=1000)] if pend else rds.xreadgroup(f"{WORD_PREFIX}{stream}{WORD_PREFIX}{stream}","C"+str(stream),{f"{WORD_PREFIX}{stream}":">"},5000)
		if word is not None and word!=[]:
			if len(word[0])<=1:
				rds.xack(f"{WORD_PREFIX}{stream}",f"{WORD_PREFIX}{stream}{WORD_PREFIX}{stream}", word[0][0][0])
				return
			wordids,wordtexts=getwords(word[0][1])
			for c,wordtext in enumerate(wordtexts):
				rds.zincrby(WORDSET,1,wordtext)
				rds.xack(f"{WORD_PREFIX}{stream}",f"{WORD_PREFIX}{stream}{WORD_PREFIX}{stream}", wordids[c])
		else:
			return
	except:
		print("error while countwords")
