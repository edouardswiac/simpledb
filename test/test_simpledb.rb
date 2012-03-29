require 'test/unit'
require 'simpledb'

class TestSimpleDB < Test::Unit::TestCase
  
  def setup
    @db = SimpleDB::Database.new
    @db.set(:one, 1)
    @db.set(:ten, 10)
    @db.set(:twenty, 20)
    @db.set(:cat, "black")
  end

  def test_keys_exist
    assert @db.key?(:cat)
    assert !@db.key?(:dog)
  end

  def test_set
    @db.set(:cow, "black")
    assert_equal "black", @db.get(:cow)
  end

  def test_get
    assert_equal 1, @db.get(:one)
    assert_equal 10, @db.get(:ten)
    assert_equal 20, @db.get(:twenty)
    assert_equal "black", @db.get(:cat)
    assert_nil @db.get(:dog)
  end

  def test_unset
    @db.unset :ten
    assert_equal false, @db.key?(:ten)
    assert_nil @db.get(:ten)
  end

  def test_begin_transaction
    assert !@db.in_transaction?
    @db.begin_transaction
    assert @db.in_transaction?
  end

  def test_rollback_no_transaction
    assert_raise (Exception) { @db.rollback_transaction }
  end

  def test_rollback_simple_transaction
    begin_tx_id = @db.begin_transaction
    assert @db.in_transaction?

    @db.set(:cat, "white")
    @db.set(:dog, "black")

    rollback_tx_id = @db.rollback_transaction
    assert !@db.in_transaction?

    assert_equal begin_tx_id, rollback_tx_id
    assert_equal "black", @db.get(:cat)
    assert_nil @db.get(:dog)
  end

  def test_rollback_in_nested_transactions
    tx1 = @db.begin_transaction
    @db.set(:one, 0)
    
    tx2 = @db.begin_transaction
    @db.set(:ten, 100)

    tx3 = @db.begin_transaction
    @db.set(:twenty, 17)

    tx3_r = @db.rollback_transaction
    assert_equal 20, @db.get(:twenty)

    tx2_r = @db.rollback_transaction
    assert_equal 10, @db.get(:ten)

    tx1_r = @db.rollback_transaction
    assert_equal 1, @db.get(:one)

    assert_equal tx1, tx1_r
    assert_equal tx2, tx2_r
    assert_equal tx3, tx3_r
  end

  def test_commit_single_transaction
    @db.begin_transaction
    @db.set(:cat, "white")
    @db.set(:fifty, 50)
    @db.commit_transactions

    assert_equal "white", @db.get(:cat)
    assert_equal 50, @db.get(:fifty)
  end

  def test_commit_multiple_transactions
    @db.begin_transaction
    @db.set(:cat, "grey")
    
    @db.begin_transaction
    @db.set(:dog, "golden")

    @db.begin_transaction
    @db.set(:ten, "dix")

    assert_equal "grey", @db.get(:cat)
    assert_equal "golden", @db.get(:dog)
    assert_equal "dix", @db.get(:ten)

    @db.commit_transactions
    assert_equal "grey", @db.get(:cat)
    assert_equal "golden", @db.get(:dog)
    assert_equal "dix", @db.get(:ten)
  end

  def test_rollback_in_nested_transactions
    @db.begin_transaction
    @db.set(:cat, "grey")
    
    @db.begin_transaction
    @db.set(:one, 0)
    @db.rollback_transaction
    assert_equal 1, @db.get(:one)

    @db.begin_transaction
    @db.set(:ten, "dix")

    assert_equal "grey", @db.get(:cat)
    assert_equal "dix", @db.get(:ten)

    @db.commit_transactions
    assert_equal "grey", @db.get(:cat)
    assert_equal 1, @db.get(:one)
    assert_equal "dix", @db.get(:ten)
  end

  def test_unset_in_nested_transactions
    @db.begin_transaction
    @db.set(:cat, "grey")
    
    @db.begin_transaction
    @db.set(:one, 0)

    @db.begin_transaction
    @db.set(:ten, "dix")
    @db.unset(:one)

    assert_equal "grey", @db.get(:cat)
    assert_equal "dix", @db.get(:ten)

    @db.commit_transactions
    assert_equal "grey", @db.get(:cat)
    assert_equal nil, @db.get(:one)
    assert_equal "dix", @db.get(:ten)
  end

  def test_scenario1
    @db.begin_transaction                   # BEGIN
    @db.set(:a, 10)                         #   SET a 10
    assert_equal 10, @db.get(:a)            #   GET a (=10)
    @db.begin_transaction                   #   BEGIN
    @db.set(:a, 20)                         #     SET a 20 
    assert_equal 20, @db.get(:a)            #     GET a (=20)
    @db.rollback_transaction                #     ROLLBACK
    assert_equal 10, @db.get(:a)            #   GET a (=10)
    @db.rollback_transaction                #   ROLLBACK
    assert_nil @db.get(:a)                  # GET a (=NULL)
  end

  def test_scenario2     
    @db.begin_transaction                   # BEGIN
    @db.set(:a, 30)                         #   SET a 30
    @db.begin_transaction                   #   BEGIN
    @db.set(:a, 40)                         #     SET a 40
    @db.commit_transactions                 # COMMIT
    assert_equal 40, @db.get(:a)            # GET a (=40)
    assert_raise (Exception) do             # ROLLBACK (Exception)
      @db.rollback_transaction 
    end
  end

  def test_scenario3
    @db.set(:a, 50)                       # SET a 50
    @db.begin_transaction                 # BEGIN
    assert_equal 50, @db.get(:a)          #   GET a (=50)
    @db.set(:a, 60)                       #   SET a 60
    @db.begin_transaction                 #   BEGIN
    @db.unset(:a)                         #     UNSET a
    assert_nil @db.get(:a)                #     GET a (=NULL)
    @db.rollback_transaction              #     ROLLBACK
    assert_equal 60, @db.get(:a)          #   GET a (=60)
    @db.commit_transactions               # COMMIT
    assert_equal 60, @db.get(:a)          # GET a (=60)
  end
end