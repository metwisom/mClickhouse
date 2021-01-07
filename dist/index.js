"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.mClickhouse = void 0;
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
    ping() {
        const options = {
            hostname: this.settings.hostname,
            port: this.settings.port,
            path: encodeURI('/ping'),
            method: 'GET',
        };
        return new Promise((resolve, reject) => {
            http_1.default
                .get(options, (res) => {
                let data = '';
                res
                    .on('data', (chunk) => data += chunk)
                    .on('end', () => resolve(data === 'Ok.\n'));
            })
                .on('error', (e) => {
                reject(`problem with request: ${e.message}`);
            });
        });
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
            .match(/^insert/);
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
                    incomeData.count += new_data.length;
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
    query(query) {
        return this.sendQuery(query);
    }
}
exports.mClickhouse = mClickhouse;
