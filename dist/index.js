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
        };
    }
    buildOption(path, method = "GET") {
        return {
            ...this.settings,
            path: encodeURI(path),
            method: method,
        };
    }
    ping() {
        const options = this.buildOption('/ping');
        return new Promise((resolve, reject) => {
            http_1.default
                .get(options)
                .on('response', response => resolve(response.statusCode == 200))
                .on('error', error => reject(`problem with request: ${error.message}`));
        });
    }
    query(body, payload = '') {
        const options = this.buildOption('/?query=' + body, 'POST');
        const incomeData = new QueryResult_1.default();
        let incomeBody = '';
        const isInsert = !!body
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
            req.write(payload);
            req.end();
        });
    }
    insert(table, data) {
        return new Promise((resolve, reject) => {
            let rows = '';
            if (Array.isArray(data) && Array.isArray(data[0])) {
                rows = data.map((row) => row.join('\t')).join('\n');
            }
            else if (Array.isArray(data) && !Array.isArray(data[0])) {
                rows = data.join('\t');
            }
            else {
                reject(new TypeError('array expected'));
            }
            resolve(this.query('INSERT INTO ' + table + ' FORMAT TabSeparated', rows));
        });
    }
}
exports.mClickhouse = mClickhouse;
