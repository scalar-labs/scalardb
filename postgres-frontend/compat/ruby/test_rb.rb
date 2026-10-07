require 'active_record'
def step(name)
  r = yield
  puts "ok   #{name} #{r.inspect[0, 200]}"
rescue => e
  puts "FAIL #{name} #{e.class}: #{e.message.lines.first.to_s.strip[0, 200]}"
end
step('connect') do
  ActiveRecord::Base.establish_connection(adapter: 'postgresql', host: 'localhost', port: 15444, database: 'orm', username: 'postgres', prepared_statements: true)
  ActiveRecord::Base.connection.execute('SELECT 1').to_a
end
conn = ActiveRecord::Base.connection
step('create_table') do
  conn.drop_table(:widgets, if_exists: true)
  conn.create_table(:widgets, id: :integer) { |t| t.string :name; t.float :price; t.integer :qty; t.boolean :active; t.datetime :created_at }
  conn.tables
end
class Widget < ActiveRecord::Base; end
step('create') { Widget.create!(id: 1, name: 'a', price: 1.5, qty: 2, active: true, created_at: Time.utc(2024, 1, 1)); Widget.create!(id: 2, name: 'b', price: 2.5, qty: 3, active: false); Widget.count }
step('where in') { Widget.where(id: [1, 2]).order(:id).pluck(:id, :name, :active) }
step('find/update') { w = Widget.find(1); w.update!(price: 9.5); Widget.find(1).price }
step('transaction') { Widget.transaction { Widget.create!(id: 3, name: 'c'); Widget.where('price > ?', 0).count } }
step('destroy') { Widget.find(2).destroy; Widget.count }
step('columns') { Widget.columns.map { |c| [c.name, c.type] } }
step('indexes') { conn.indexes(:widgets).map(&:name) }
step('primary key') { conn.primary_key(:widgets) }
step('schema dump bits') { [conn.table_exists?(:widgets), conn.column_exists?(:widgets, :name)] }
