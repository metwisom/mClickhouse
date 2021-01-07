import http from 'http';
import QueryResponse from './QueryResponse';
import QueryResult from './QueryResult';


export class mClickhouse {
  settings = {
    hostname: 'localhost',
    port: 8123,
    method: 'POST',
  };

  ping() {
    const options = {
      hostname: this.settings.hostname,
      port: this.settings.port,
      path: encodeURI('/ping'),
      method: 'GET',
    };
    return new Promise((resolve, reject) => {
      http
        .get(options, (res) => {
          let data = '';
          res
            .on('data', (chunk) => data += chunk)
            .on('end', () => resolve(data === 'Ok.\n'));
        })
        .on('error', (e) => {
          reject(`problem with request: ${e.message}`);
        });
    })

  }

  sendQuery(query: string, body = '') {
    const options = {
      hostname: this.settings.hostname,
      port: this.settings.port,
      path: encodeURI('/?query=' + query),
      method: 'POST',
    };

    const incomeData = new QueryResult();

    let incomeBody = '';
    const isInsert = !!query
      .toLowerCase()
      .trim()
      .match(/^insert/);

    return new Promise((resolve, reject) => {

      const req = http.request(options, (res) => {
        const response = new QueryResponse();
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
          incomeData.add(
            ...new_data.map((data) => data.split('\t'))
          );

        });
        res.on('end', () => {
          response.total = process.hrtime(requestStart).toString();
          if (isInsert) {
            response.payload = incomeBody;
          } else {
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

  insert(table: string, data: unknown) {
    let rows = '';
    if (Array.isArray(data) && Array.isArray(data[0])) {
      const cols = data.map((row) => row.join('\t'));
      rows = cols.join('\n');
    } else if (Array.isArray(data) && !Array.isArray(data[0])) {
      rows = data.join('\t');
    } else {
      throw 'expected Array';
    }
    return this.sendQuery(
      'INSERT INTO ' + table + ' FORMAT TabSeparated',
      rows
    );
  }

  query(query: string) {
    return this.sendQuery(query);
  }
}
