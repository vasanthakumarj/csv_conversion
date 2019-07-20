from fastavro import writer, reader, parse_schema
import csv


schema = {"namespace": "csv to avro",
		  "type": "record",
		  "name": "csv",
		  "fields": [
			  {"name": "id", "type": "int", "default":0},
			  {"name": "name",  "type": "string", "default":"aaa"},
			  {"name": "lastname", "type": "string", "default":"cccc"}
		  ]
		  }
parsed_schema = parse_schema(schema)

csv_inp = r"C:\Users\jvasa\IdeaProjects\csv_conversion\csv_conversion\source\input.csv"
#avro_schema = r"C:\Users\<directory>\output_avro_schema.avsc"
avro_output = r"C:\Users\jvasa\IdeaProjects\csv_conversion\csv_conversion\source\output.avro"

records=[]
with open(csv_inp, 'r') as csvinp:
	rdr = csv.reader(csvinp)
	header = next(rdr)
	for row in rdr:
		records.append((dict(zip(header, [int(row[0]), row[1], row[2]]))))


print(records)
# Writing
with open(avro_output, 'wb') as out:
	writer(out, parsed_schema, records)

# Reading
with open(avro_output, 'rb') as fo:
	for record in reader(fo):
		print(record)
