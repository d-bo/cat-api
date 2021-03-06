# cat-api

## Install
```bash
pip install -r requirements.txt
```

### Docker

```bash
# Start
cd parser-ilde
sudo docker build --no-cache -t gapple/cat-api .
# !!! network host -> localhost MongoDB
sudo docker run --network host -d --restart always --log-driver syslog gapple/cat-api:latest
# Stop
sudo docker ps
sudo docker kill <image_name>
```

## MongoDB
### Index
```sql
db.gestori_new.createIndex({Name_e': 'text'})
db.gestori.createIndex({"Artic": 1, "Cod_good": 1, "Barcod": 1})
db.gestori.createIndex({'Name': 'text', 'Name_e': 'text', 'Artic': 'text', 'Cod_good': 'text'})
db['RIVE_products_final'].createIndex({'name': 'text', 'name_e': 'text', 'brand': 'text'})
db['letu_products_final'].createIndex({'name': 'text', 'desc': 'text', 'brand': 'text'})
db['ILDE_products_final'].createIndex({'pn': 'text', 'articul': 'text', 'brand': 'text'})
db['PODR_products_final'].createIndex({'name': 'text', 'articul': 'text', 'brand': 'text'})
```

### MongoDB Docker container
```sh
# Debugging: remove -d param
sudo docker run --network host -d --restart always --log-driver syslog -v /var/lib/mongodb:/data/db mongo
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
db.createUser(
   {
     user: "sedova_w",
     pwd: "sedova_w",
     roles:
       [
         { role: "readWrite", db: "parser" }
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

# IMPORT GESTORI
# 1
mongoimport --host localhost --username apidev --password "apidev" --collection gestori_up --db parser --file /home/administrator/ArtPriceKatalog.csv --type csv --fields Artic,Cod_good,Name_e,Name,Barcod,Retail_price,name_brand --ignoreBlanks



# ArtPriceKatalog.sh
# 
# Deps: https://github.com/topkecleon/telegram-bot-bash

DATE=`date '+%Y-%m-%d %H:%M:%S'|md5sum|awk {'print $1'}`
cd /mnt

sudo tmux new-window "cd /home/administrator/dev/telegram-bot-bash/ && bash bashbot.sh broadcast \"ArtPriceKatalog[$DATE]: Started ...\""
cp -f ArtPriceKatalog.csv ArtPriceKatalog_copy.csv

sudo sed -i -e 's/\,/\ /g' ArtPriceKatalog.csv
sudo sed -i -e 's/\;/\,/g' ArtPriceKatalog.csv
sudo sed -i -e 's/\"/\ /g' ArtPriceKatalog.csv

sudo mongo localhost:27017/parser ArtPriceKatalogCopy.js

mongoimport --host localhost --username apidev --password "apidev" --collection gestori_rc --db parser --file ArtPriceKatalog.csv --type csv --fields Artic,Cod_good,Name_$

sudo tmux new-window "cd /home/administrator/dev/telegram-bot-bash/ && bash bashbot.sh broadcast \"ArtPriceKatalog[$DATE]: filter NumberLong gestori_rc ...\""
sudo mongo localhost:27017/parser ArtPriceKatalogFilter.js

sudo tmux new-window "cd /home/administrator/dev/telegram-bot-bash/ && bash bashbot.sh broadcast \"ArtPriceKatalog[$DATE]: Finished.\""



# ArtPriceKatalogCopy.js
db.gestori_rc.renameCollection("gestori_rc_bkp");



# ArtPriceKatalogFilter.js
db.gestori_rc.remove({Artic: "Artic"});
db.gestori_rc.find({Barcod: {$exists: true}}).forEach(function(obj) {
    obj.Barcod = obj.Barcod.valueOf().toString();
    db.gestori_rc.save(obj);
});
db.gestori_rc.find({Cod_good: {$exists: true}}).forEach(function(obj) {
    obj.Cod_good = obj.Cod_good.valueOf().toString();
    db.gestori_rc.save(obj);
});
db['letu_products_final_bkp'].find({listingprice: {$eq: "default"}}).forEach(function(obj) {
    obj.listingprice = "";
    db['letu_products_final_bkp'].save(obj);
});



# Artic,Cod_good,Name_e,Name,Barcod,Price_Kalinka,Price_Chelyabinsk,Price_Germes,Price_Kazan,Price_Novosib,Price_Kristall,Price_Kuncevo,Price_afimoll,Price_Passage,Price_Kristall,Price_Samara,Price_Rostov,cod_brand,name_brand

```

```js

// IMPORT GESTORI
// #2
// Convert mongo NumberLong field to string
db.gestori_rc1.find({Barcod: {$exists: true}}).forEach(function(obj) {
    obj.Barcod = obj.Barcod.valueOf().toString();
    db.gestori_rc1.save(obj);
});
db.gestori_rc1.find({Cod_good: {$exists: true}}).forEach(function(obj) {
    obj.Cod_good = obj.Cod_good.valueOf().toString();
    db.gestori_rc1.save(obj);
});
// Convert matched fields
db.matched.find({gest_match_code: {$exists: true}}).forEach(function(obj) { 
    obj.gest_match_code = obj.gest_match_code.valueOf().toString();
    db.matched.save(obj);
});
```

```sql
# Clear matched gestori
db['gestori_up'].update({Artic: {$exists: true}}, {$unset: {'rive_match_code': '', 'ilde_match_code': '', 'letu_match_code': ''}}, {multi: true})

# Clear matched rive, ilde, letu
db['letu_products_final'].update({gest_match_code: {$exists: true}}, {$unset: {'gest_match_code': ''}})
db['RIVE_products_final'].update({gest_match_code: {$exists: true}}, {$unset: {'gest_match_code': ''}})
db['ILDE_products_final'].update({gest_match_code: {$exists: true}}, {$unset: {'gest_match_code': ''}})
```

## Redis
```sh
sudo docker run --network host -d --restart always --log-driver syslog --name gapple-redis redis
```