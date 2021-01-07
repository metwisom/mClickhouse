"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const http_1 = __importDefault(require("http"));
const QueryResponse_1 = __importDefault(require("./QueryResponse"));
const QueryResult_1 = __importDefault(require("./QueryResult"));
class mClickhouse {
    constructor() {
        this.settings = {
            hostname: 'localhost',
            port: 8123,
            method: 'POST',
        };
    }
    sendQuery(query, body = '') {
        const options = {
            hostname: this.settings.hostname,
            port: this.settings.port,
            path: encodeURI('/?query=' + query),
            method: 'POST',
        };
        const incomeData = new QueryResult_1.default();
        let incomeBody = '';
        const isInsert = !!query
            .toLowerCase()
            .trim()
            .match(/insert/);
        return new Promise((resolve, reject) => {
            const req = http_1.default.request(options, (res) => {
                const response = new QueryResponse_1.default();
                response.status = res.statusCode;
                response.headers = res.headers;
                const requestStart = process.hrtime();
                res.setEncoding('utf8');
                res.on('data', (chunk) => {
                    if (isInsert) {
                        incomeBody += chunk;
                        return;
                    }
                    incomeBody += chunk;
                    const new_data = incomeBody.split('\n');
                    incomeBody = new_data.splice(-1, 1).toString();
                    incomeData.add(...new_data.map((data) => data.split('\t')));
                });
                res.on('end', () => {
                    response.total = process.hrtime(requestStart).toString();
                    if (isInsert) {
                        response.payload = incomeBody;
                    }
                    else {
                        response.payload = incomeData;
                    }
                    resolve(response);
                });
            });
            req.on('error', (e) => {
                reject(`problem with request: ${e.message}`);
            });
            req.write(body);
            req.end();
        });
    }
    insert(table, data) {
        let rows = '';
        if (Array.isArray(data) && Array.isArray(data[0])) {
            const cols = data.map((row) => row.join('\t'));
            rows = cols.join('\n');
        }
        else if (Array.isArray(data) && !Array.isArray(data[0])) {
            rows = data.join('\t');
        }
        else {
            throw 'expected Array';
        }
        return this.sendQuery('INSERT INTO ' + table + ' FORMAT TabSeparated', rows);
    }
    select(query) {
        return this.sendQuery(query);
    }
}
const ch = new mClickhouse();
const inport = [];
(async () => {
    for (let i = 0; i < 2000000; i++)
        inport.push(['4', 9848, '2020-01-02 14:28:56']);
    const a = await ch.insert('some_table', inport);
    console.log(a);
})();
(async () => {
    const a = await ch.select('SELECT count(*) FROM some_table WHERE data = \'4\'');
    console.log(a);
})();
