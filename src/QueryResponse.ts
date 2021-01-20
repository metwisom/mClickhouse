import { IncomingHttpHeaders } from 'http';
import QueryResult from './QueryResult';

export default class QueryResponse {
  status?: number = -1;
  headers?: IncomingHttpHeaders;
  total?: string;
  payload: QueryResult | string;
  constructor() {
    this.payload = '';
  }
}
