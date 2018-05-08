echo "start runing the data processor"
echo "default memory chunk size 100MB"
echo "default number of threads 4"
echo "default number of files for one pass merge is 5"

java s57zhao.sortLargeFile.DataProcessor "data/enwik9.txt" "output9" 100 4 5