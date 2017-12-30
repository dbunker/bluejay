Bluejay
=======

This repository contains a veriety of data analysis scripts. Projects located in [spark](/spark/) are launched with [deploy.py](/deploy.py) which can build, test, upload jar file, start [EMR](https://aws.amazon.com/elasticmapreduce/) [Spark](http://spark.apache.org/) cluster, and run the job. Though the file [config_example.py](/config_example.py) must first be copied to config.py with the values filled in for the AWS account. An example command is below for building, uploading, starting cluster, and submitting the new job for the [word_count](/spark/word_count/) project.

```
python deploy.py --project word_count --build --job_upload --job_submit_cluster_full
```

## NLP

The first project is for natural language processing using Spark and [CoreNLP](http://stanfordnlp.github.io/CoreNLP/) on the [2015 Reddit comments corpus](https://archive.org/details/2015_reddit_comments_corpus). More information on this is available in this [blog post](http://dbunker.github.io/2016/01/04/spark-and-nlp-on-reddit/).

The Spark and CoreNLP code is in the [nlp](/spark/nlp/) project. As it is a hefty processing step, CoreNLP is used first with training data added to the jar uploaded to EMR. This step performs tokenization, tagging, stemming, and named entity recognition. Input and output s3 folders are listed in [deploy.py](/deploy.py). The file [RC_2015-05](/examples/2015/data/comments/RC_2015-05) can be used or swapped with a file with more data for local testing, which can also be done using [deploy.py](/deploy.py).

The next step in the machine learning pipeline is the [word_count](/spark/word_count/) project. This does the simple text parsing and outputs the json containing information about organization, subreddit, count, direct adjectives, and connected adjectives to be added to [react-tabulator](https://github.com/dbunker/react-tabulator). The file [parse.py](/process/nlp/parse.py) can be used to perform the conversion from Spark output to table input json.

## Word2Vec

The next project is for Word2Vec from Spark on the Reddit comment corpus. More information on this is available in this [blog post](http://dbunker.github.io/2016/01/05/spark-word2vec-on-reddit/).

This continues the pipeline from [word_count](/spark/word_count/) and runs Spark's [Word2Vec](http://spark.apache.org/docs/latest/ml-features.html#word2vec) and outputs organization, subreddit, count, and similarities. Similarities lists both the words and cosine similarity metrics. [parse.py](/process/nlp/parse.py) can also be used to create output json for [react-tabulator](https://github.com/dbunker/react-tabulator).
