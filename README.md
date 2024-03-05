## Produce test data

### produce CSV data as Avro

prerequisites

`pip install pandas`
`pip install confluent-kafka`

e.g. 

`python csvtoavroproducer.py --topic csv_avro --config ccloud.mvzwd2.props.json --schema_file customer.avro --key_field CustomerID --csv_file CustomerMasterData.csv`