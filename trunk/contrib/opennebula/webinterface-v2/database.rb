Sequel.extension :migration

DB = Sequel.sqlite('opennebula.db')

DB.create_table? :services do
  primary_key :id
  DataTime    :created_at
  DataTime    :updated_at
  String      :master_node
end

DB.create_table? :vms do
  primary_key :id
  String      :one_vm_id
  foreign_key :service_id, :services
end

# models just work ...
class Service < Sequel::Model
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
  many_to_one :services
end
