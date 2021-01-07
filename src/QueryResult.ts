export default class QueryResult {
  rows: string[][] = [];
  count = 0;
  add(...data: string[][]): void {
    this.rows.push(...data);
  }
}