from fastavro import writer, reader, parse_schema
import csv


schema = {"namespace": "csv to avro",
 "type": "record",
 "name": "csv",
 "fields": [
     {"name": "id", "type": "int"},
     {"name": "name",  "type": "string"},
     {"name": "lastname", "type": "string"}
 ]
}
parsed_schema = parse_schema(schema)

csv_inp = r"C:\Users\<directory>\output.csv"
avro_schema = r"C:\Users\<directory>\output_avro_schema.avsc"
avro_output = r"C:\Users\<directory>\output.avro" 

records=[]
with open(csv_inp, 'r') as csvinp:
	rdr = csv.reader(csvinp)
	header = next(rdr)
	for row in rdr:
		records.append((dict(zip(header, [int(row[0]), row[1], row[2]]))))

# Writing
with open(avro_output, 'wb') as out:
    writer(out, parsed_schema, records)

# Reading
with open(avro_output, 'rb') as fo:
    for record in reader(fo):
        print(record)
