# @metwisom/mclickhouse
NodeJS client for [ClickHouse](https://clickhouse.yandex/).
Send query over HTTP interface.

***
Install:

```bash
npm i @metwisom/mclickhouse
```
***

Example:

```javascript
const { mClickhouse } = require('@metwisom/mclickhouse');

const clickhouse = new mClickhouse();
```


***
 
Exec query:
```javascript
await clickhouse.query(`DROP TABLE IF EXISTS default.test`);
await clickhouse.query(
    `CREATE TABLE default.test (
        data String, 
        some_index UInt32, 
        some_date DateTime('Europe/Moscow')
    ) 
    ENGINE = MergeTree()
    PARTITION BY toYYYYMM(some_date)
    ORDER BY some_index
    SETTINGS index_granularity = 8192`);

const preparedData = Array(10).fill().map(
    () => [getRandomInt(1000), getRandomInt(1000), '2020-01-02 14:28:56']
);

// mono insert
await clickhouse.insert('default.test', preparedData[0]);

// multi insert
await clickhouse.insert('default.test', preparedData);

const res = await clickhouse.query('SELECT * FROM default.test LIMIT 10');
````

***

**Run Tests**:

```
npm install

npm run test
# or verbose mode
npm run testf
```

