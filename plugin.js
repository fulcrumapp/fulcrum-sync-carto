import Plugin from 'fulcrum-sync-plugin';
import request from 'request';
import Promise from 'bluebird';

import Schema from 'fulcrum-schema/dist/schema';
import sqldiff from 'sqldiff';
import V2 from 'fulcrum-schema/dist/schemas/postgres-query-v2';

const {SchemaDiffer, Sqlite, Postgres} = sqldiff;

const reqPromise = Promise.promisify(request);
const req = (options) => reqPromise({forever: true, ...options});

const RECORD_BATCH_SIZE = 1000;

export default class Carto extends Plugin {
  get enabled() {
    return false;
  }

  async runTask({app, yargs}) {
    this.args = yargs.usage('Usage: carto --org [org] --form [form name] --apikey [api key] --user [user]')
      .demandOption([ 'org', 'form', 'apikey', 'user' ])
      .argv;

    const account = await app.fetchAccount(this.args.org);

    if (account) {
      const forms = await account.findActiveForms({});

      for (const form of forms) {
        if (form.name !== this.args.form) {
          continue;
        }

        try {
          await this.updateForm(form, account, this.formVersion(form), null);
        } catch (ex) {
          // ignore errors
        }

        await this.updateForm(form, account, null, this.formVersion(form));

        let statements = [];

        const execStatementBatch = async () => {
          if (statements.length === 0) {
            return;
          }

          await this.run(statements.join('\n'));

          statements = [];
        };

        await form.findEachRecord({}, async (record) => {
          await record.getForm();

          process.stdout.write('.');

          const recordStatements = this.updateRecordStatements(record);

          statements.push.apply(statements, recordStatements);

          if (statements.length % 1000) {
            await execStatementBatch();
          }
        });

        await execStatementBatch();
      }
    } else {
      console.error('Unable to find account', this.args.org);
    }
  }

  async initialize({app}) {
    app.on('form:save', this.onFormSave);
    app.on('record:save', this.onRecordSave);
    app.on('record:delete', this.onRecordDelete);

    this.args = app.args;

    const response = await this.run('SELECT cdb_usertables AS name FROM CDB_UserTables()');

    const rows = response.body.rows;

    this.tableNames = rows.map(o => o.name);

    console.log('Existing Tables', '\n  ' + this.tableNames.join('\n  '));

    this.pgdb = new app.api.Postgres({});
  }

  run = async (sql) => {
    const options = {
      url: `https://${this.args.user}.carto.com/api/v2/sql`,
      method: 'POST',
      json: {
        q: sql,
        api_key: this.args.apikey
      }
    };

    return await req(options);
  }

  log = (...args) => {
    // console.log(...args);
  }

  tableName = (account, name) => {
    return 'carto_' + account.rowID + '_' + name;
  }

  onFormSave = async ({form, account, oldForm, newForm}) => {
    await this.updateForm(form, account, oldForm, newForm);
  }

  onRecordSave = ({record}) => {
    // this.log('record updated', record.displayValue);
  }

  onRecordDelete = ({record}) => {
    // this.log('record deleted', record.displayValue);
  }

  updateRecord = async (record) => {
    await this.run(this.updateRecordStatements(record).join('\n'));
  }

  updateRecordStatements = async (record) => {
    const statements = this.app.api.PostgresRecordValues.updateForRecordStatements(this.pgdb, record);

    return statements.map(o => o.sql);
  }

  updateForm = async (form, account, oldForm, newForm) => {
    const rootTableName = this.tableName(account, 'form_' + form.rowID);

    if (this.tableNames.indexOf(rootTableName) === -1) {
      oldForm = null;
    }

    const {statements, newSchema} = await this.updateFormTableStatements(account, oldForm, newForm);

    // const cartoConvertStatements = newSchema.tables.map((view) => {
    //   return "select cdb_cartodbfytable('" + this.tableName(account, view.name) + "');";
    // });

    const cartoConvertStatements = [];

    const cartoStatements = [
      ...statements,
      ...cartoConvertStatements
    ];

    console.log('CARTO', cartoStatements.join('\n'));

    const response = await this.run(cartoStatements.join('\n'));

    console.log(response.body);
  }

  async updateFormTableStatements(account, oldForm, newForm) {
    let oldSchema = null;
    let newSchema = null;

    if (oldForm) {
      oldSchema = new Schema(oldForm, V2, null);
    }

    if (newForm) {
      newSchema = new Schema(newForm, V2, null);
    }

    const differ = new SchemaDiffer(oldSchema, newSchema);

    let generator = new Postgres(differ, {afterTransform: null});

    generator.tablePrefix = 'carto_' + account.rowID + '_';

    const statements = generator.generate();

    return {statements, oldSchema, newSchema};
  }

  formVersion = (form) => {
    if (form == null) {
      return null;
    }

    return {
      id: form._id,
      row_id: form.rowID,
      name: form._name,
      elements: form._elementsJSON
    };
  }
}
