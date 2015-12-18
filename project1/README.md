# README

Team 6: Buzun Chen, Zhuoren Shen, Shu Chen	

Please run the following command to test our program for B.2:

	python run.py
	
We included 2 python libraries to accomplish the task: `csv` and `json`.

**Python CSV** is used to read and write .csv files. We manually did type casting while reading to ensure there are double quotes around non-numeric values in the output files.

**Python JSON** is used to convert each line of data to a string to be used as the key in a hash map structure (python set). We used this approach to elliminate duplicates in decomposited tables.
	
2015/10/02