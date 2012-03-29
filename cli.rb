require 'simpledb'


puts "SimpleDB command line interface v0.1"
db = SimpleDB::Database.new

while true
  '> '.display
  input = gets.chomp.split(" ")
  cmd = input.shift
  case 
    when cmd == 'GET' then
      output = db.get input[0]
      if output.nil?
        puts "NULL"
      else
        puts output
      end
    when cmd == 'SET' then
      puts db.set input[0], input[1]
    when cmd == 'BEGIN' then
      t_id = db.begin_transaction
      puts "Starting transaction ##{t_id}"
    when cmd == 'COMMIT' then
      db.commit_transactions
      puts "Transactions were commited"
    when cmd == 'ROLLBACK' then
      t_id = db.rollback_transaction
      puts "Rollback of transaction ##{t_id}"
    when cmd == 'EXIT' then
      exit
    else
      puts "Unknown command, type EXIT to quit."
  end
end

trap("SIGINT") { exit}
