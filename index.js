var http = require("http");

class mClickhouse {
  constructor() {
    this.settings = {
      hostname: "localhost",
      port: 8123,
      method: "POST",
    };
  }

  sendQuery(query, body) {
    let options = {
      hostname: this.settings.hostname,
      port: this.settings.port,
      path: encodeURI("/?query=" + query),
      method: "POST",
    };
    let incomeData = [];

    let incomeBody = "";
    let isInsert = !!query
      .toLowerCase()
      .trim()
      .match(/insert/);

    return new Promise((resolve, reject) => {
      const response = {}
      const req = http.request(options, (res) => {
        response.status = res.statusCode;
        response.headers = res.headers;
        const requestStart = process.hrtime()
        res.setEncoding("utf8");
        res.on("data", (chunk) => {
          if (isInsert) {
            incomeBody += chunk;
          } else {
            incomeBody += chunk;
            let new_data = incomeBody.split("\n");
            incomeBody = new_data.splice(-1, 1);
            incomeData = incomeData.concat(
              new_data.map((data) => data.split("\t"))
            );
          }
        });
        res.on("end", () => {
          response.total = process.hrtime(requestStart) + ''
          if (isInsert) {
            response.payload = incomeBody;
          } else {
            response.payload = incomeData;
          }
          resolve(response);
        });
      });

      req.on("error", (e) => {
        reject(`problem with request: ${e.message}`);
      });
      req.write(body);
      req.end();
    });
  }

  insert(table, data) {
    if (Array.isArray(data) && Array.isArray(data[0])) {
      data = data.map((row) => row.join("\t"));
      data = data.join("\n");
    } else if (Array.isArray(data) && Array.isArray(data[0])) {
      data = data.join("\n");
    } else {
      throw "expected Array";
    }
    return this.sendQuery(
      "INSERT INTO " + table + " FORMAT TabSeparated",
      data
    );
  }

  select(query) {
    return this.sendQuery(query, "");
  }
}

const ch = new mClickhouse();

let inport = [];

(async () => {
  for (let i = 0; i < 2000000; i++)
    inport.push(["4", 9848, "2020-01-02 14:28:56"]);

  let a = await ch.insert("some_table", inport);
  console.log(a);
})();

(async () => {
  let a = await ch.select("SELECT count(*) FROM some_table WHERE data = '4'");
  console.log(a);
})();
