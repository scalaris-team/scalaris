
set :database, 'sqlite://contrail.db'

puts "the scalaris table doesn't exist" if !database.table_exists?('scalaris')


# define database migrations. pending migrations are run at startup and
# are guaranteed to run exactly once per database.
migration "create scalaris table" do
  database.create_table :scalaris do
    primary_key :id
    String      :user
    String      :known_nodes
    DataTime    :created_at
    DataTime    :updated_at
  end
end

migration "create vms table" do
  database.create_table :vms do
    primary_key :id
    String      :one_vm_id
    foreign_key :scalaris_id, :scalaris
  end
end
# you can also alter tables
#migration "everything's better with bling" do
#  database.alter_table :foos do
#    drop_column :baz
#    add_column :bling, :float
#  end
#end

# models just work ...
class Scalaris < Sequel::Model
  one_to_many :vms

  def before_create
    return false if super == false
    self.created_at = Time.now.utc
  end

  def before_update
    return false if super == false
    self.updated_at = Time.now.utc
  end
end

class Vm < Sequel::Model
  many_to_one :scalaris
end
