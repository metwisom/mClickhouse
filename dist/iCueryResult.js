"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
class QueryResult {
    constructor() {
        this.rows = new Array();
        this.count = 0;
    }
    add(...data) {
        this.rows.push(data);
    }
}
exports.default = QueryResult;
