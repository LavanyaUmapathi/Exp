#!/bin/sh
#
# Extract sample records from csv.gz file
#

N=10000

data_dir=data

src=billing_events_20140430.csv.gz
dest=10k.csv

table=billing_events

echo "Writing first $N records of $src to $dest"
# Turns ,, into ,\N, for MySQL to read them as NULLs.  Run twice to
# handle multiple sequences of ,,
gunzip < $data_dir/$src | head -$N | sed -e 's/,,/,\\N,/g' -e 's/,,/,\\N,/g' > $data_dir/$dest

# Print what is needed to load it
echo "LOAD DATA LOCAL INFILE '$dest' INTO TABLE \`$table\` FIELDS ENCLOSED BY '\"' TERMINATED BY ',';"
