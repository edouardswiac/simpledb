class SimpleDB::Database
  def initialize
    @tx = [] # first is the oldest tx, last the most recent
    @data = {} # database implemention as a hash. B-tree in real life :)
  end

  def set(key, value)
    if in_transaction?
      @tx.last[key] = value
    else
      @data[key] = value
    end
  end

  def get(key)
    @tx.reverse_each do |t|
      return t[key] if t.has_key?(key)
    end
    @data[key]
  end

  def unset(key)
    if in_transaction?
      @tx.last[key] = nil
    else
      @data.delete(key)
    end
  end

  def key?(key)
    @data.has_key?(key)
  end

  def size
    @data.size
  end

  def begin_transaction
    h = Hash.new
    @tx.push h
    h.object_id
  end

  def commit_transactions
    @tx.each do |t|
      @data.merge!(t)
    end
    @tx = []
  end

  def current_transaction_id
    @tx.last.object_id
  end

  def rollback_transaction
    raise Exception, 'INVALID ROLLBACK' if not in_transaction?
    t = @tx.pop 
    t.object_id
  end

  def in_transaction?
    return (not @tx.empty?)
  end
end