Sequel.extension :migration

DB = Sequel.sqlite('opennebula.db')

DB.create_table? :scalaris do
  primary_key :id
  DataTime    :created_at
  DataTime    :updated_at
  String      :head_node
end

DB.create_table? :scalarisvms do
  primary_key :id
  String      :one_vm_id
  foreign_key :scalaris_id, :scalaris
end

DB.create_table? :hadoops do
  primary_key :id
  String      :user
  String      :known_nodes
  DataTime    :created_at
  DataTime    :updated_at
  String      :master_node
end

DB.create_table? :hadoopvms do
  primary_key :id
  String      :one_vm_id
  foreign_key :hadoop_id, :hadoops
end

# models just work ...
class Scalaris < Sequel::Model
  one_to_many :scalarisvms

  def before_create
    return false if super == false
    self.created_at = Time.now.utc
  end

  def before_update
    return false if super == false
    self.updated_at = Time.now.utc
  end
end

class Scalarisvm < Sequel::Model
  many_to_one :scalaris
end

class Hadoop < Sequel::Model
  one_to_many :hadoopvms

  def before_create
    return false if super == false
    self.created_at = Time.now.utc
  end

  def before_update
    return false if super == false
    self.updated_at = Time.now.utc
  end
end

class Hadoopvm < Sequel::Model
  many_to_one :hadoops
end
