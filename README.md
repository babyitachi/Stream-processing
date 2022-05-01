# Stream-processing

Our machine has Redis, RabbitMQ and Celery installed.

Redis : key-value store
RabbitMQ : message queuing
Celery : worker manager

We have implemented a word-count application for counting the words in the tweets of given dataset.
The data is stored in the Redis store.
We do stream processing, as tweet is read, the words of all the tweets are being counted parallely.
