const { mClickhouse } = require('../dist/index')
const getRandomInt = (max) => Math.floor(Math.random() * Math.floor(max));
const testTableName = String.fromCharCode(...Array(10).fill(0).map(e => getRandomInt(26) + 97));

const inport = []
for (let i = 0; i < 10; i++)
  inport.push([getRandomInt(1000), getRandomInt(1000), '2020-01-02 14:28:56']);

test('ping', async () => {
  const ch = new mClickhouse()
  expect(await ch.ping()).toBe(true);
});

test('create', async () => {
  const ch = new mClickhouse()
  expect((await ch.query(`CREATE TABLE default.${testTableName} (data String, some_index UInt32, some_date DateTime('Europe/Moscow')) ENGINE = MergeTree() PARTITION BY toYYYYMM(some_date) ORDER BY some_index SETTINGS index_granularity = 8192`)).status).toBe(200);
});

test('insert', async () => {
  const ch = new mClickhouse()
  expect((await ch.insert('some_table', inport)).status).toBe(200);
});

test('select', async () => {
  const ch = new mClickhouse()
  expect((await ch.query('SELECT * FROM some_table LIMIT 10')).payload.count).toBe(10);
});

test('drop', async () => {
  const ch = new mClickhouse()
  expect((await ch.query(`DROP TABLE default.${testTableName}`)).status).toBe(200);
});