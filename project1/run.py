import csv
import json

# open file
csvfile = open("SalesFile.csv", "r")

# read file, preserve all quotes
reader = csv.reader(csvfile, quoting=csv.QUOTE_NONE)

# init BCNF tables as sets (unique entry)
store = set()
weekdate = set()
attributes = set()
sales = set()

# create constant for column names
Store = 0
Dept = 1
WeekDate = 2
WeeklySales = 3
IsHoliday = 4
Type = 5
Size = 6
Temperature = 7
FuelPrice = 8
CPI = 9
UnemploymentRate = 10

# traverse
i = 0
for row in reader:
	# special case: table headers
	if (i == 0):
		store_1st_row = json.dumps([row[Store], row[Size], row[Type]])
		weekdate_1st_row = json.dumps([row[WeekDate], row[IsHoliday]])
		attributes_1st_row = json.dumps([row[Store], row[WeekDate], row[Temperature], row[FuelPrice], row[CPI], row[UnemploymentRate]])
		sales_1st_row = json.dumps([row[Store], row[WeekDate], row[Dept], row[WeeklySales]])
		i += 1
		continue
	# do type convertion while reading
	store_row = json.dumps([row[Store], row[Size], row[Type]])
	weekdate_row = json.dumps([row[WeekDate], row[IsHoliday]])
	attributes_row = json.dumps([row[Store], row[WeekDate], row[Temperature], row[FuelPrice], row[CPI], row[UnemploymentRate]])
	sales_row = json.dumps([row[Store], row[WeekDate], row[Dept], row[WeeklySales]])
	# uniquely add to set
	store.add(store_row)
	weekdate.add(weekdate_row)
	attributes.add(attributes_row)
	sales.add(sales_row)

# create new file
store_file = open("Store.csv", "w")
# init csv writer, disabled quoting
store_writer = csv.writer(store_file, escapechar='\\', quotechar='', quoting=csv.QUOTE_NONE)
# write header
store_writer.writerow(json.loads(store_1st_row))

weekdate_file = open("WeekDate.csv", "w")
weekdate_writer = csv.writer(weekdate_file, escapechar='\\', quotechar='', quoting=csv.QUOTE_NONE)
weekdate_writer.writerow(json.loads(weekdate_1st_row))

attributes_file = open("Attributes.csv", "w")
attributes_writer = csv.writer(attributes_file, escapechar='\\', quotechar='', quoting=csv.QUOTE_NONE)
attributes_writer.writerow(json.loads(attributes_1st_row))

sales_file = open("Sales.csv", "w")
sales_writer = csv.writer(sales_file, escapechar='\\', quotechar='', quoting=csv.QUOTE_NONE)
sales_writer.writerow(json.loads(sales_1st_row))

# write to files
for row in store:
	store_writer.writerow(json.loads(row))
for row in weekdate:
	weekdate_writer.writerow(json.loads(row))
for row in attributes:
	attributes_writer.writerow(json.loads(row))
for row in sales:
	sales_writer.writerow(json.loads(row))

