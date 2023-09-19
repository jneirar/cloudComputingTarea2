FROM gettyimages/spark:1.5.1-hadoop-2.6

WORKDIR /app

COPY wordcount.py /app/wordcount.py

VOLUME [ "/data" ]

CMD ["spark-submit", "/app/wordcount.py", "/data/movies.csv", "/data/output_file.txt"]
