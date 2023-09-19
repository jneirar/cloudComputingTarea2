from pyspark import SparkContext
import sys
import os
import shutil
import time
import re

if len(sys.argv) != 3:
    print("Usage: wordcount <input> <output>")
    exit(-1)

input_path = sys.argv[1]
output_path = sys.argv[2]

if os.path.exists(output_path):
    os.remove(output_path)

secs = 10
for i in range(secs, 0, -1):
    print("Iniciando en " + str(i) + " seg")
    time.sleep(1)
print("\nIniciado\n")

sc = SparkContext(appName="WordCount")
lines = sc.textFile(input_path)
words = lines.flatMap(lambda line: re.findall(r'\b\w+\b', line))
word_counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
sorted_word_counts = word_counts.map(lambda wc: (wc[1], wc[0])).sortByKey(False)
temp_output_path = output_path + "_temp"
sorted_word_counts.coalesce(1).saveAsTextFile(temp_output_path)

sc.stop()

os.rename(os.path.join(temp_output_path, "part-00000"), output_path)
shutil.rmtree(temp_output_path)

for i in range(secs, 0, -1):
    print("Terminando en " + str(i) + " seg")
    time.sleep(1)
print("\nTerminado\n")
