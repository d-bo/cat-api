## MongoDB
```sql
db.gestori.createIndex({"Artic": 1, "Cod_good": 1, "Barcod": 1})
db.gestori.createIndex({'Name': 'text', 'Name_e': 'text'})
db['RIVE_products_final'].createIndex({'name': 'text', 'desc': 'text'})
db['letu_products_final'].createIndex({'name': 'text', 'desc': 'text', 'articul': 'text'})
db['ILDE_products_final'].createIndex({'pn': 'text', 'pc': 'text', 'articul': 'text', 'brand': 'text'})
```

## Import brands
```bash
mongoimport -d parser -c all_brands --type csv --file brends.csv --fields id,val --ignoreBlanks
```