const { mClickhouse } = require("../dist/index");
const getRandomInt = (max) => Math.floor(Math.random() * Math.floor(max));
const testTable = String.fromCharCode(
  ...Array(10)
    .fill(0)
    .map((e) => getRandomInt(26) + 97)
);

const inport = [];
for (let i = 0; i < 10; i++)
  inport.push([getRandomInt(1000), getRandomInt(1000), "2020-01-02 14:28:56"]);

const ch = new mClickhouse();

test("ping", async () => {
  expect.assertions(1);
  await expect(ch.ping()).resolves.toBe(true);
});

test("ping_error", async () => {
  expect.assertions(1);
  const brokenCh = new mClickhouse();
  brokenCh.settings.hostname = "127.0.0.0";
  await expect(brokenCh.ping()).rejects.toMatch(
    "problem with request: connect ENETUNREACH 127.0.0.0:8123 - Local (0.0.0.0:0)"
  );
});

test("create", async () => {
  expect.assertions(1);
  await expect(
    ch.query(
      `CREATE TABLE default.${testTable} (data String, some_index UInt32, some_date DateTime('Europe/Moscow')) ENGINE = MergeTree() PARTITION BY toYYYYMM(some_date) ORDER BY some_index SETTINGS index_granularity = 8192`
    )
  ).resolves.toHaveProperty("status", 200);
});

test("insert_mono", async () => {
  expect.assertions(1);
  await expect(ch.insert(testTable, inport[0])).resolves.toHaveProperty(
    "status",
    200
  );
});

test("insert_multi", async () => {
  expect.assertions(1);
  await expect(ch.insert(testTable, inport)).resolves.toHaveProperty("status", 200);
});

test("insert_error(not_array)", async () => {
  expect.assertions(1);
  expect(ch.insert(testTable, "")).rejects.toThrowError("array expected");
});

test("select", async () => {
  expect.assertions(1);
  await expect(
    ch.query(`SELECT * FROM default.${testTable} LIMIT 10`)
  ).resolves.toHaveProperty("payload");
});

test("select_error", async () => {
  expect.assertions(1);
  const brokenCh = new mClickhouse();
  brokenCh.settings.hostname = "127.0.0.0";
  await expect(
    brokenCh.query(`SELECT * FROM default.${testTable} LIMIT 10`)
  ).rejects.toMatch(
    "problem with request: connect ENETUNREACH 127.0.0.0:8123 - Local (0.0.0.0:0)"
  );
});

test("drop", async () => {
  expect.assertions(1);
  await expect(ch.query(`DROP TABLE default.${testTable}`)).resolves.toHaveProperty(
    "status",
    200
  );
});
