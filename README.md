# cat-api

## Install
```bash
pip install -r requirements.txt
```

### Docker

```bash
# Start
cd parser-ilde
sudo docker build -t ga/cat-api .
# !!! network host -> localhost MongoDB
sudo docker run --network host -d --restart always --log-driver syslog ga/cat-api:latest
# Stop
sudo docker ps
sudo docker kill <image_name>
```

## MongoDB
### Index
```sql
db.gestori_new.createIndex({'Name': 'text', 'Name_e': 'text', 'Artic': 'text', 'Cod_good': 'text', 'Barcod': 'text', 'cod_brand': 'text'})
db.gestori.createIndex({"Artic": 1, "Cod_good": 1, "Barcod": 1})
db.gestori.createIndex({'Name': 'text', 'Name_e': 'text'})
db['RIVE_products_final'].createIndex({'name': 'text', 'desc': 'text', 'name_e': 'text', 'code': 'text'})
db['letu_products_final'].createIndex({'name': 'text', 'desc': 'text', 'articul': 'text'})
db['ILDE_products_final'].createIndex({'pn': 'text', 'pc': 'text', 'articul': 'text', 'brand': 'text'})
```

### Users
```sql
use admin
db.createUser(
    {
      user: "superuser",
      pwd: "12345678",
      roles: [ "root" ]
    }
)
use parser
db.createUser(
   {
     user: "sedova",
     pwd: "sedova",
     roles:
       [
         { role: "read", db: "parser" }
       ]
   }
)
```
```sh
sudo vi /etc/mongod.conf
```
```conf
# network interfaces
net:
  port: 27017
#  bindIp: 127.0.0.1

security:
  authorization: 'enabled'
```
```sh
#restart
sudo service mongod restart
# auth
mongo -u superuser -p --authenticationDatabase admin
```

## Import brands
```bash
mongoimport -d parser -c all_brands --type csv --file brends.csv --fields id,val --ignoreBlanks
```
```bash
mongoimport --username sedova --password sedova --authenticationDatabase parser -d parser -c all_brands --type csv --file brnds.csv --fields id,val,val_en --ignoreBlanks
```
```bash
mongoimport --host localhost --username apidev --password "apidev" --collection gestori_new --db parser --file /home/administrator/ArtPriceKatalog.csv --type csv --fields Artic,Cod_good,Name_e,Name,Barcod,Retail_price,cod_brand,name_brand --ignoreBlanks
```