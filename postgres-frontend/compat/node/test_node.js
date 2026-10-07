const { Client } = require('pg');
const { Sequelize, DataTypes } = require('sequelize');
async function step(name, fn) {
  try { const r = await fn(); console.log('ok  ', name, r === undefined || typeof r === 'function' ? '' : String(JSON.stringify(r)).slice(0, 200)); }
  catch (e) { console.log('FAIL', name, String(e.message || e).split('\n')[0].slice(0, 200)); }
}
(async () => {
  const c = new Client({ host: 'localhost', port: 15444, database: 'orm', user: 'postgres' });
  await c.connect();
  await step('version', () => c.query('SELECT version()').then(r => r.rows[0]));
  await step('create', () => c.query('DROP TABLE IF EXISTS node_items').then(() => c.query('CREATE TABLE node_items (id int PRIMARY KEY, name text, qty int, created timestamptz)')));
  await step('insert', () => c.query('INSERT INTO node_items (id, name, qty, created) VALUES ($1, $2, $3, $4)', [1, 'a', 10, new Date('2024-01-01T00:00:00Z')]).then(r => r.rowCount));
  await step('insert2', () => c.query('INSERT INTO node_items (id, name, qty, created) VALUES ($1, $2, $3, $4)', [2, 'b', 20, new Date('2024-06-01T12:30:00Z')]).then(r => r.rowCount));
  await step('any array param', () => c.query('SELECT id, name, qty, created FROM node_items WHERE id = ANY($1) ORDER BY id', [[1, 2]]).then(r => r.rows));
  await step('tx', async () => { await c.query('BEGIN'); await c.query('UPDATE node_items SET qty = qty + 1 WHERE id = $1', [1]); await c.query('COMMIT'); return (await c.query('SELECT qty FROM node_items WHERE id = 1')).rows; });
  await step('named prepared', async () => { for (let i = 0; i < 3; i++) await c.query({ name: 'get', text: 'SELECT name FROM node_items WHERE id = $1', values: [i + 1] }); return 'ok'; });
  await step('returning', () => c.query('INSERT INTO node_items (id, name, qty) VALUES ($1, $2, $3) RETURNING id, name', [3, 'c', 0]).then(r => r.rows));
  await c.end();
  const sequelize = new Sequelize('orm', 'postgres', '', { host: 'localhost', port: 15444, dialect: 'postgres', logging: false });
  const Widget = sequelize.define('widget', { id: { type: DataTypes.INTEGER, primaryKey: true }, name: DataTypes.STRING, price: DataTypes.DOUBLE, active: DataTypes.BOOLEAN }, { timestamps: false });
  await step('seq authenticate', () => sequelize.authenticate());
  await step('seq sync force', () => Widget.sync({ force: true }));
  await step('seq bulkCreate', () => Widget.bulkCreate([{ id: 1, name: 'x', price: 1.5, active: true }, { id: 2, name: 'y', price: 2.5, active: false }]).then(r => r.length));
  await step('seq findAll in', () => Widget.findAll({ where: { id: [1, 2] }, order: [['id', 'ASC']] }).then(r => r.map(w => w.toJSON())));
  await step('seq update', () => Widget.update({ price: 3.5 }, { where: { id: 1 } }));
  await step('seq findOne', () => Widget.findOne({ where: { name: 'x' } }).then(w => w && w.toJSON()));
  await step('seq destroy', () => Widget.destroy({ where: { id: 2 } }));
  await step('seq transaction', () => sequelize.transaction(async t => { await Widget.create({ id: 3, name: 'z', price: 0, active: true }, { transaction: t }); return Widget.count({ transaction: t }); }));
  await step('seq describeTable', () => sequelize.getQueryInterface().describeTable('widgets'));
  await step('seq showIndex', () => sequelize.getQueryInterface().showIndex('widgets').then(r => r.map(i => i.name)));
  await step('seq showAllTables', () => sequelize.getQueryInterface().showAllTables());
  await sequelize.close();
})();
