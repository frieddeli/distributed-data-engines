#!/usr/bin/env python
# coding: utf-8

# #![Spark Logo](http://spark-mooc.github.io/web-assets/images/ta_Spark-logo-small.png) + ![Python Logo](http://spark-mooc.github.io/web-assets/images/python-logo-master-v3-TM-flattened_small.png)
# 
# # **Word Count Pipeline: Building a Word Count Application**
# 
# This lab will build on the techniques covered in the Spark tutorial to develop a simple word count application.  The volume of unstructured text in existence is growing dramatically, and Spark is an excellent tool for analyzing this type of data.  In this lab, we will write code that calculates the most common words in the [Complete Works of William Shakespeare](http://www.gutenberg.org/ebooks/100) retrieved from [Project Gutenberg](https://www.gutenberg.org/).  This could also be scaled to find the most common words on the Internet.
# 
# ** During this lab we will cover: **
# - *Part 1*: Creating RDDs
# - *Part 2*: Counting with Pair RDDs
# - *Part 3*: More Transformations
# - *Part 4*: Applying to a File
# 
# > Note that, for reference, you can look up the details of the relevant methods in [Spark's Python API](https://spark.apache.org/docs/latest/api/python/reference/index.html)

# #### ** Prerequisite **

# ** (a) Installing PySpark **

# In[1]:


get_ipython().system(' pip install pyspark==3.5.5')


# ** (b) Initiating a SparkContext **

# In[2]:


# import pyspark and initiate a SparkContext
from pyspark import SparkConf, SparkContext

# stop any existing context before creating a new one
existing = SparkContext._active_spark_context
if existing:
    existing.stop()

conf = SparkConf().setAppName('Spark Programming')
sc = SparkContext(conf=conf)


# ** (c) Initializing the Test class **

# In[3]:


# Load in the testing code and check to see if your answer is correct
# If incorrect it will report back '1 test failed' for each failed test
# Make sure to rerun any cell you change before trying the test again

# initialize the Test class
import hashlib

class TestFailure(Exception):
    pass
class PrivateTestFailure(Exception):
    pass

class Test(object):
    passed = 0
    numTests = 0
    failFast = False
    private = False

    @classmethod
    def setFailFast(cls):
        cls.failFast = True

    @classmethod
    def setPrivateMode(cls):
        cls.private = True

    @classmethod
    def assertTrue(cls, result, msg=""):
        cls.numTests += 1
        if result == True:
            cls.passed += 1
            print("1 test passed.")
        else:
            print("1 test failed. " + msg)
            if cls.failFast:
                if cls.private:
                    raise PrivateTestFailure(msg)
                else:
                    raise TestFailure(msg)

    @classmethod
    def assertEquals(cls, var, val, msg=""):
        cls.assertTrue(var == val, msg)

    @classmethod
    def assertEqualsHashed(cls, var, hashed_val, msg=""):
        cls.assertEquals(cls._hash(var), hashed_val, msg)

    @classmethod
    def printStats(cls):
        print("{0} / {1} test(s) passed.".format(cls.passed, cls.numTests))

    @classmethod
    def _hash(cls, x):
        return hashlib.sha1(str(x).encode('utf-8')).hexdigest()


# #### ** Part 1: Creating RDDs **
# 
# In this part of the lab, we will explore creating a base RDD with `parallelize` and using pair RDDs to count words.
# 
# ** (1a) Create a base RDD **
# 
# We'll start by generating a base RDD by using a Python list and the `sc.parallelize` method.  Then we'll print out the type of the base RDD.

# In[4]:


wordsList = ['cat', 'elephant', 'rat', 'rat', 'cat']
wordsRDD = sc.parallelize(wordsList, numSlices=4)
# Print out the type of wordsRDD
print(type(wordsRDD))


# ** (1b) Capitalize and test **
# 
# 

# In[5]:


def capitalize(word):
    """Capitalize input word

    Args:
        word (str): A string.

    Returns:
        str: A string with all letters capitalized
    """
    # just use the built-in upper() method to make everything uppercase
    return word.upper()

print(capitalize('cat'))


# In[6]:


# One way of completing the function
def capitalize(word):
    return word.upper()

print(capitalize('cat'))


# In[7]:


# TEST Pluralize and test (1b)
Test.assertEquals(capitalize('rat'), 'RAT', 'incorrect result: capitalize does not work properly')


# ** (1c) Apply `capitalize` to the base RDD **
# 
# Now pass each item in the base RDD into a [map()](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.map.html) transformation that applies the `capitalize()` function to each element. And then call the [collect()](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.collect.html) action to see the transformed RDD.

# In[8]:


# pass the capitalize function into map so it applies to each word
capitalRDD = wordsRDD.map(capitalize)
print(capitalRDD.collect())


# In[9]:


# TEST Apply capitalize to the base RDD(1c)
Test.assertEquals(capitalRDD.collect(), ['CAT', 'ELEPHANT', 'RAT', 'RAT', 'CAT'],
                  'incorrect values for capitalRDD')


# ** (1d) Pass a `lambda` function to `map` **
# 
# Let's create the same RDD using a `lambda` function.

# In[10]:


# same as before but with a lambda instead of a named function
capitalLambdaRDD = wordsRDD.map(lambda word: word.upper())
print(capitalLambdaRDD.collect())


# In[11]:


# TEST Pass a lambda function to map (1d)
Test.assertEquals(capitalLambdaRDD.collect(), ['CAT', 'ELEPHANT', 'RAT', 'RAT', 'CAT'],
                  'incorrect values for capitalLambdaRDD (1d)')


# ** (1e) Length of each word **
# 
# Now use `map()` and a `lambda` function to return the number of characters in each word.  We'll `collect` this result directly into a variable.

# In[12]:


# use len() on each word to get its length
capitalLengths = (capitalRDD
                  .map(lambda word: len(word))
                  .collect())
print(capitalLengths)


# In[13]:


# TEST Length of each word (1e)
Test.assertEquals(capitalLengths, [3, 8, 3, 3, 3],
                  'incorrect values for pluralLengths')


# ** (1f) Pair RDDs **
# 
# The next step in writing our word counting program is to create a new type of RDD, called a pair RDD. A pair RDD is an RDD where each element is a pair tuple `(k, v)` where `k` is the key and `v` is the value. In this example, we will create a pair consisting of `('<word>', 1)` for each word element in the RDD.
# 
# We can create the pair RDD using the `map()` transformation with a `lambda()` function to create a new RDD.

# In[14]:


# create a pair rdd where each word maps to 1
wordPairs = wordsRDD.map(lambda word: (word, 1))
print(wordPairs.collect())


# In[15]:


# TEST Pair RDDs (1f)
Test.assertEquals(wordPairs.collect(),
                  [('cat', 1), ('elephant', 1), ('rat', 1), ('rat', 1), ('cat', 1)],
                  'incorrect value for wordPairs')


# #### ** Part 2: Counting with Pair RDDs **
# 
# Now, let's count the number of times a particular word appears in the RDD. There are multiple ways to perform the counting, but some are much less efficient than others.
# 
# A naive approach would be to `collect()` all of the elements and count them in the driver program. While this approach could work for small datasets, we want an approach that will work for any size dataset including terabyte- or petabyte-sized datasets. In addition, performing all of the work in the driver program is slower than performing it in parallel in the workers. For these reasons, we will use data parallel operations.
# 
# ** (2a) `groupByKey()` approach **
# 
# An approach you might first consider (we'll see shortly that there are better ways) is based on using the [groupByKey()](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.groupByKey.html) transformation. As the name implies, the `groupByKey()` transformation groups all the elements of the RDD with the same key into a single list in one of the partitions. There are two problems with using `groupByKey()`:
#   + The operation requires a lot of data movement to move all the values into the appropriate partitions.
#   + The lists can be very large. Consider a word count of English Wikipedia: the lists for common words (e.g., the, a, etc.) would be huge and could exhaust the available memory in a worker.
# 
# Use `groupByKey()` to generate a pair RDD of type `('word', iterator)`.

# In[16]:


# Note that groupByKey requires no parameters
# group all the values for the same key together into an iterator
wordsGrouped = wordPairs.groupByKey()
for key, value in wordsGrouped.collect():
    print('{0}: {1}'.format(key, list(value)))


# In[17]:


# TEST groupByKey() approach (2a)
Test.assertEquals(sorted(wordsGrouped.mapValues(lambda x: list(x)).collect()),
                  [('cat', [1, 1]), ('elephant', [1]), ('rat', [1, 1])],
                  'incorrect value for wordsGrouped')


# ** (2b) Use `groupByKey()` to obtain the counts **
# 
# Using the `groupByKey()` transformation creates an RDD containing 3 elements, each of which is a pair of a word and a Python iterator.
# 
# Now sum the iterator using a `map()` transformation.  The result should be a pair RDD consisting of (word, count) pairs.

# In[18]:


# sum up all the 1s in each group to get the count per word
wordCountsGrouped = wordsGrouped.map(lambda x: (x[0], sum(x[1])))
print(wordCountsGrouped.collect())


# In[19]:


# TEST Use groupByKey() to obtain the counts (2b)
Test.assertEquals(sorted(wordCountsGrouped.collect()),
                  [('cat', 2), ('elephant', 1), ('rat', 2)],
                  'incorrect value for wordCountsGrouped')


# ** (2c) Count using `reduceByKey` **
# 
# A better approach is to start from the pair RDD and then use the [reduceByKey()](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.reduceByKey.html) transformation to create a new pair RDD. The `reduceByKey()` transformation gathers together pairs that have the same key and applies the function provided to two values at a time, iteratively reducing all of the values to a single value. `reduceByKey()` operates by applying the function first within each partition on a per-key basis and then across the partitions, allowing it to scale efficiently to large datasets.

# In[20]:


# Note that reduceByKey takes in a function that accepts two values and returns a single value
# just add the two counts together to reduce them
wordCounts = wordPairs.reduceByKey(lambda a, b: a + b)
print(wordCounts.collect())


# In[21]:


# TEST Counting using reduceByKey (2c)
Test.assertEquals(sorted(wordCounts.collect()), [('cat', 2), ('elephant', 1), ('rat', 2)],
                  'incorrect value for wordCounts')


# ** (2d) All together **
# 
# The expert version of the code performs the `map()` to pair RDD, `reduceByKey()` transformation, and `collect` in one statement.

# In[22]:


# chain the map and reduceByKey together in one go
wordCountsCollected = (wordsRDD
                       .map(lambda word: (word, 1))
                       .reduceByKey(lambda a, b: a + b)
                       .collect())
print(wordCountsCollected)


# In[23]:


# TEST All together (2d)
Test.assertEquals(sorted(wordCountsCollected), [('cat', 2), ('elephant', 1), ('rat', 2)],
                  'incorrect value for wordCountsCollected')


# #### ** Part 3: More Transformations **
# 
# ** (3a) Unique words **
# 
# Calculate the number of unique words in `wordsRDD`.  You can use other RDDs that you have already created to make this easier.

# In[24]:


# count() gives the number of distinct (word, count) pairs which equals unique words
uniqueWords = wordCounts.count()
print(uniqueWords)


# In[25]:


# TEST Unique words (3a)
Test.assertEquals(uniqueWords, 3, 'incorrect count of uniqueWords')


# ** (3b) Calculate mean using `reduce` **
# 
# Find the mean number of words per unique word in `wordCounts`.
# 
# Use a `reduce()` action to sum the counts in `wordCounts` and then divide by the number of unique words.  First `map()` the pair RDD `wordCounts`, which consists of (key, value) pairs, to an RDD of values.

# In[26]:


from operator import add
# first grab just the counts (the values), then add them all up
totalCount = (wordCounts
              .map(lambda x: x[1])
              .reduce(add))
# divide by the number of unique words to get the mean
average = totalCount / float(uniqueWords)
print(totalCount)
print(round(average, 2))


# In[27]:


# TEST Mean using reduce (3b)
Test.assertEquals(round(average, 2), 1.67, 'incorrect value of average')


# #### ** Part 4: Applying to a File **
# 
# In this section we will finish developing our word count application.  We'll have to build the `wordCount` function, deal with real world problems like capitalization and punctuation, load in our data source, and compute the word count on the new data.
# 
# ** (4a) `wordCount` function **
# 
# First, define a function for word counting.  You should reuse the techniques that have been covered in earlier parts of this lab.  This function should take in an RDD that is a list of words like `wordsRDD` and return a pair RDD that has all of the words and their associated counts.

# In[28]:


def wordCount(wordListRDD):
    """Creates a pair RDD with word counts from an RDD of words.

    Args:
        wordListRDD (RDD of str): An RDD consisting of words.

    Returns:
        RDD of (str, int): An RDD consisting of (word, count) tuples.
    """
    # map each word to a (word, 1) pair then reduce to sum up the counts
    return wordListRDD.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

print(wordCount(wordsRDD).collect())


# In[29]:


# TEST wordCount function (4a)
Test.assertEquals(sorted(wordCount(wordsRDD).collect()),
                  [('cat', 2), ('elephant', 1), ('rat', 2)],
                  'incorrect definition for wordCount function')


# ** (4b) Capitalization and punctuation **
# 
# Real world files are more complicated than the data we have been using in this lab. Some of the issues we have to address are:
#   + Words should be counted independent of their capitialization (e.g., Spark and spark should be counted as the same word).
#   + All punctuation should be removed.
#   + Any leading or trailing spaces on a line should be removed.
# 
# Define the function `removePunctuation` that converts all text to lower case, removes any punctuation, and removes leading and trailing spaces.  Use the Python [re](https://docs.python.org/2/library/re.html) module to remove any text that is not a letter, number, or space. Reading `help(re.sub)` might be useful.

# In[30]:


import re
def removePunctuation(text):
    """Removes punctuation, changes to lower case, and strips leading and trailing spaces.

    Note:
        Only spaces, letters, and numbers should be retained.  Other characters should should be
        eliminated (e.g. it's becomes its).  Leading and trailing spaces should be removed after
        punctuation is removed.

    Args:
        text (str): A string.

    Returns:
        str: The cleaned up string.
    """
    # first make everything lowercase
    text = text.lower()
    # remove anything that is not a letter, number, or space
    text = re.sub(r'[^a-z0-9 ]', '', text)
    # strip any extra whitespace from the front and back
    return text.strip()

print(removePunctuation('Hi, you!'))
print(removePunctuation(' No under_score!'))


# In[31]:


# TEST Capitalization and punctuation (4b)
Test.assertEquals(removePunctuation(" The Elephant's 4 cats. "),
                  'the elephants 4 cats',
                  'incorrect definition for removePunctuation function')


# ** (4c) Load a text file **
# 
# For the next part of this lab, we will use the [Complete Works of William Shakespeare](http://www.gutenberg.org/ebooks/100) from [Project Gutenberg](http://www.gutenberg.org). To convert a text file into an RDD, we use the `SparkContext.textFile()` method. We also apply the recently defined `removePunctuation()` function using a `map()` transformation to strip out the punctuation and change all text to lowercase.  Since the file is large we use `take(15)`, so that we only print 15 lines.
# 
# **WARNING**: If you haven't uploaded the text file as instructed in the warmup notebook, please upload it before running the cell below.

# In[32]:


# Just run this code
import os.path
filePath = 'shakespere.txt'

shakespeareRDD = (sc
                  .textFile(filePath, 8)
                  .map(removePunctuation))
print('\n'.join(shakespeareRDD
                .zipWithIndex() # to (line, lineNum)
                .map(lambda s: '{0}: {1}'.format(s[1], s[0])) # to 'lineNum: line'
                .take(15)))


# ** (4d) Words from lines **
# 
# Before we can use the `wordcount()` function, we have to split each line by its spaces. Apply a transformation that will split each element of the RDD by its spaces. For each element of the RDD, you should apply Python's string [split()](https://docs.python.org/3/library/string.html#string.split) function. You might think that a `map()` transformation is the way to do this, but think about what the result of the `split()` function will be.

# In[33]:


# use flatMap instead of map so each line gets split and the lists are flattened into one rdd
shakespeareWordsRDD = shakespeareRDD.flatMap(lambda line: line.split())
shakespeareWordCount = shakespeareWordsRDD.count()
print(shakespeareWordsRDD.top(5))
print(shakespeareWordCount)


# In[34]:


# TEST Words from lines (4d)
Test.assertTrue(shakespeareWordCount == 959524,
                'incorrect value for shakespeareWordCount')
Test.assertEquals(shakespeareWordsRDD.top(5),
                  [u'zwounds', u'zwounds', u'zwounds', u'zwounds', u'zwounds'],
                  'incorrect value for shakespeareWordsRDD')


# ** (4e) Count the words **
# 
# We now have an RDD that is only words.  Next, let's apply the `wordCount()` function to produce a list of word counts. We can view the top 10 words by using the `takeOrdered()` action; however, since the elements of the RDD are pairs, we need a custom sort function that sorts using the value part of the pair.
# 
# You'll notice that many of the words are common English words. These are called stopwords. In a later lab, we will see how to eliminate them from the results.
# 
# Use the `wordCount()` function and `takeOrdered()` to obtain the ten most common words and their counts.

# In[35]:


# use takeOrdered with a negative key to sort by count descending, grab top 10
top10WordsAndCounts = wordCount(shakespeareWordsRDD).takeOrdered(10, key=lambda x: -x[1])
print('\n'.join(map(lambda s: '{0}: {1}'.format(s[0], s[1]), top10WordsAndCounts)))


# In[36]:


# TEST Count the words (4f)
Test.assertEquals(top10WordsAndCounts,
                  [(u'the', 30018), (u'and', 28358), (u'i', 21868), (u'to', 20890),
                   (u'of', 18815), (u'a', 16000), (u'you', 14438), (u'my', 13191),
                   (u'in', 12034), (u'that', 11781)],
                  'incorrect value for top10WordsAndCounts')

