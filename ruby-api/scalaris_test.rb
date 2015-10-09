#!/usr/bin/ruby -KU
# Copyright 2011 Zuse Institute Berlin
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

require "test/unit"
begin
  require "#{File.expand_path(File.dirname(__FILE__))}/scalaris"
rescue LoadError => e
  raise unless e.message =~ /scalaris/
  require "scalaris"
end

$_TEST_DATA = [
             "ahz2ieSh", "wooPhu8u", "quai9ooK", "Oquae4ee", "Airier1a", "Boh3ohv5", "ahD3Saog", "EM5ooc4i", 
             "Epahrai8", "laVahta7", "phoo6Ahj", "Igh9eepa", "aCh4Lah6", "ooT0ath5", "uuzau4Ie", "Iup6mae6", 
#             "xie7iSie", "ail8yeeP", "ooZ4eesi", "Ahn7ohph", "Ohy5moo6", "xooSh9Oo", "ieb6eeS7", "Thooqu9h", 
#             "eideeC9u", "phois3Ie", "EimaiJ2p", "sha6ahR1", "Pheih3za", "bai4eeXe", "rai0aB7j", "xahXoox6", 
#             "Xah4Okeg", "cieG8Yae", "Pe9Ohwoo", "Eehig6ph", "Xe7rooy6", "waY2iifu", "kemi8AhY", "Che7ain8", 
#             "ohw6seiY", "aegh1oBa", "thoh9IeG", "Kee0xuwu", "Gohng8ee", "thoh9Chi", "aa4ahQuu", "Iesh5uge", 
#             "Ahzeil8n", "ieyep5Oh", "xah3IXee", "Eefa5qui", "kai8Muuf", "seeCe0mu", "cooqua5Y", "Ci3ahF6z", 
#             "ot0xaiNu", "aewael8K", "aev3feeM", "Fei7ua5t", "aeCa6oph", "ag2Aelei", "Shah1Pho", "ePhieb0N", 
#             "Uqu7Phup", "ahBi8voh", "oon3aeQu", "Koopa0nu", "xi0quohT", "Oog4aiph", "Aip2ag5D", "tirai7Ae", 
#             "gi0yoePh", "uay7yeeX", "aeb6ahC1", "OoJeic2a", "ieViom1y", "di0eeLai", "Taec2phe", "ID2cheiD", 
#             "oi6ahR5M", "quaiGi8W", "ne1ohLuJ", "DeD0eeng", "yah8Ahng", "ohCee2ie", "ecu1aDai", "oJeijah4", 
#             "Goo9Una1", "Aiph3Phi", "Ieph0ce5", "ooL6cae7", "nai0io1H", "Oop2ahn8", "ifaxae7O", "NeHai1ae", 
#             "Ao8ooj6a", "hi9EiPhi", "aeTh9eiP", "ao8cheiH", "Yieg3sha", "mah7cu2D", "Uo5wiegi", "Oowei0ya", 
#             "efeiDee7", "Oliese6y", "eiSh1hoh", "Joh6hoh9", "zib6Ooqu", "eejiJie4", "lahZ3aeg", "keiRai1d", 
#             "Fei0aewe", "aeS8aboh", "hae3ohKe", "Een9ohQu", "AiYeeh7o", "Yaihah4s", "ood4Giez", "Oumai7te", 
#             "hae2kahY", "afieGh4v", "Ush0boo0", "Ekootee5", "Ya8iz6Ie", "Poh6dich", "Eirae4Ah", "pai8Eeme", 
#             "uNah7dae", "yo3hahCh", "teiTh7yo", "zoMa5Cuv", "ThiQu5ax", "eChi5caa", "ii9ujoiV", "ge7Iekui",
             "sai2aiTa", "ohKi9rie", "ei2ioChu", "aaNgah9y", "ooJai1Ie", "shoh0oH9", "Ool4Ahya", "poh0IeYa", 
             "Uquoo0Il", "eiGh4Oop", "ooMa0ufe", "zee6Zooc", "ohhao4Ah", "Uweekek5", "aePoos9I", "eiJ9noor", 
             "phoong1E", "ianieL2h", "An7ohs4T", "Eiwoeku3", "sheiS3ao", "nei5Thiw", "uL5iewai", "ohFoh9Ae"]

$_TOO_LARGE_REQUEST_SIZE = 1024*1024*10 # number of bytes

class TestTransactionSingleOp < Test::Unit::TestCase
  def setup
    @testTime = (Time.now.to_f * 1000).to_i
  end
  
  # Test method for TransactionSingleOp()
  def testTransactionSingleOp1()
    conn = Scalaris::TransactionSingleOp.new()
    conn.close_connection()
  end

  # Test method for TransactionSingleOp(conn)
  def testTransactionSingleOp2()
    conn = Scalaris::TransactionSingleOp.new(conn = Scalaris::JSONConnection.new(url = Scalaris::DEFAULT_URL))
    conn.close_connection()
  end

  # Test method for TransactionSingleOp.close_connection() trying to close the connection twice.
  def testDoubleClose()
    conn = Scalaris::TransactionSingleOp.new()
    conn.close_connection()
    conn.close_connection()
  end

  # Test method for TransactionSingleOp.read(key)
  def testRead_NotFound()
    key = "_Read_NotFound"
    conn = Scalaris::TransactionSingleOp.new()
    assert_raise( Scalaris::NotFoundError ) { conn.read(@testTime.to_s + key) }
    conn.close_connection()
  end

  # Test method for TransactionSingleOp.read(key) with a closed connection.
  def testRead_NotConnected()
    key = "_Read_NotConnected"
    conn = Scalaris::TransactionSingleOp.new()
    conn.close_connection()
    #assert_raise( Scalaris::ConnectionError ) { conn.read(@testTime.to_s + key) }
    assert_raise( Scalaris::NotFoundError ) { conn.read(@testTime.to_s + key) }
    conn.close_connection()
  end

  # Test method for TransactionSingleOp.write(key, value=str()) with a closed connection.
  def testWriteString_NotConnected()
    key = "_WriteString_NotConnected"
    conn = Scalaris::TransactionSingleOp.new()
    conn.close_connection()
    #assert_raise( Scalaris::ConnectionError ) { conn.write(@testTime.to_s + key, $_TEST_DATA[0]) }
    conn.write(@testTime.to_s + key, $_TEST_DATA[0])
    conn.close_connection()
  end

  # Test method for TransactionSingleOp.write(key, value=str()) and TransactionSingleOp.read(key).
  # Writes strings and uses a distinct key for each value. Tries to read the data afterwards.
  def testWriteString1()
    key = "_WriteString1_"
    conn = Scalaris::TransactionSingleOp.new()

    (0..($_TEST_DATA.length - 1)).each do |i|
      conn.write(@testTime.to_s + key + i.to_s, $_TEST_DATA[i])
    end
    
    # now try to read the data:
    (0..($_TEST_DATA.length - 1)).each do |i|
      actual = conn.read(@testTime.to_s + key + i.to_s)
      assert_equal($_TEST_DATA[i], actual)
    end
    
    conn.close_connection()
  end

  # Test method for TransactionSingleOp.write(key, value=str()) and TransactionSingleOp.read(key).
  # Writes strings and uses a single key for all the values. Tries to read the data afterwards.
  def testWriteString2()
    key = "_WriteString2"
    conn = Scalaris::TransactionSingleOp.new()

    (0..($_TEST_DATA.length - 1)).each do |i|
      conn.write(@testTime.to_s + key.to_s, $_TEST_DATA[i])
    end
    
    # now try to read the data:
    actual = conn.read(@testTime.to_s + key.to_s)
    assert_equal($_TEST_DATA[$_TEST_DATA.length - 1], actual)
    conn.close_connection()
  end

  # Test method for TransactionSingleOp.write(key, value=list()) with a closed connection.
  def testWriteList_NotConnected()
    key = "_WriteList_NotConnected"
    conn = Scalaris::TransactionSingleOp.new()
    conn.close_connection()
    #assert_raise( Scalaris::ConnectionError ) { conn.write(@testTime.to_s + key, [$_TEST_DATA[0], $_TEST_DATA[1]]) }
    conn.write(@testTime.to_s + key, [$_TEST_DATA[0], $_TEST_DATA[1]])
    conn.close_connection()
  end

  # Test method for TransactionSingleOp.write(key, value=list()) and TransactionSingleOp.read(key).
  # Writes strings and uses a distinct key for each value. Tries to read the data afterwards.
  def testWriteList1()
    key = "_WriteList1_"
    conn = Scalaris::TransactionSingleOp.new()

    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      conn.write(@testTime.to_s + key + i.to_s, [$_TEST_DATA[i], $_TEST_DATA[i + 1]])
    end
    
    # now try to read the data:
    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      actual = conn.read(@testTime.to_s + key + i.to_s)
      assert_equal([$_TEST_DATA[i], $_TEST_DATA[i + 1]], actual)
    end
    
    conn.close_connection()
  end

  # Test method for TransactionSingleOp.write(key, value=list()) and TransactionSingleOp.read(key).
  # Writes strings and uses a single key for all the values. Tries to read the data afterwards.
  def testWriteList2()
    key = "_WriteList2"
    conn = Scalaris::TransactionSingleOp.new()
    
    list = []
    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      list = [$_TEST_DATA[i], $_TEST_DATA[i + 1]]
      conn.write(@testTime.to_s + key, list)
    end
    
    # now try to read the data:
    actual = conn.read(@testTime.to_s + key)
    assert_equal(list, actual)
    conn.close_connection()
  end

  # Test method for TransactionSingleOp.test_and_set(key, oldvalue=str(), newvalue=str()) with a closed connection.
  def testTestAndSetString_NotConnected()
    key = "_TestAndSetString_NotConnected"
    conn = Scalaris::TransactionSingleOp.new()
    conn.close_connection()
    #assert_raise( Scalaris::ConnectionError ) { conn.test_and_set(@testTime.to_s + key, $_TEST_DATA[0], $_TEST_DATA[1]) }
    assert_raise( Scalaris::NotFoundError ) { conn.test_and_set(@testTime.to_s + key, $_TEST_DATA[0], $_TEST_DATA[1]) }
    conn.close_connection()
  end
  
  # Test method for TransactionSingleOp.test_and_set(key, oldvalue=str(), newvalue=str()).
  # Tries test_and_set with a non-existing key.
  def testTestAndSetString_NotFound()
    key = "_TestAndSetString_NotFound"
    conn = Scalaris::TransactionSingleOp.new()
    assert_raise( Scalaris::NotFoundError ) { conn.test_and_set(@testTime.to_s + key, $_TEST_DATA[0], $_TEST_DATA[1]) }
    conn.close_connection()
  end
  
  # Test method for TransactionSingleOp.test_and_set(key, oldvalue=str(), newvalue=str()),
  # TransactionSingleOp.read(key) and TransactionSingleOp.write(key, value=str()).
  # Writes a string and tries to overwrite it using test_and_set
  # knowing the correct old value. Tries to read the string afterwards.
  def testTestAndSetString1()
    key = "_TestAndSetString1"
    conn = Scalaris::TransactionSingleOp.new()
    
    # first write all values:
    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      conn.write(@testTime.to_s + key + i.to_s, $_TEST_DATA[i])
    end
    
    # now try to overwrite them using test_and_set:
    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      conn.test_and_set(@testTime.to_s + key + i.to_s, $_TEST_DATA[i], $_TEST_DATA[i + 1])
    end
    
    # now try to read the data:
    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      actual = conn.read(@testTime.to_s + key + i.to_s)
      assert_equal($_TEST_DATA[i + 1], actual)
    end
    
    conn.close_connection()
  end
  
  # Test method for TransactionSingleOp.test_and_set(key, oldvalue=str(), newvalue=str()),
  # TransactionSingleOp.read(key) and TransactionSingleOp.write(key, value=str()).
  # Writes a string and tries to overwrite it using test_and_set
  # knowing the wrong old value. Tries to read the string afterwards.
  def testTestAndSetString2()
    key = "_TestAndSetString2"
    conn = Scalaris::TransactionSingleOp.new()
    
    # first write all values:
    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      conn.write(@testTime.to_s + key + i.to_s, $_TEST_DATA[i])
    end
    
    # now try to overwrite them using test_and_set:
    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      begin
        conn.test_and_set(@testTime.to_s + key + i.to_s, $_TEST_DATA[i + 1], "fail")
        assert(false, 'expected a KeyChangedError')
      rescue Scalaris::KeyChangedError => exception
        assert_equal($_TEST_DATA[i], exception.old_value)
      end
    end
    
    # now try to read the data:
    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      actual = conn.read(@testTime.to_s + key + i.to_s)
      assert_equal($_TEST_DATA[i], actual)
    end
    
    conn.close_connection()
  end
  
  # Test method for TransactionSingleOp.test_and_set(key, oldvalue=str(), newvalue=list()) with a closed connection.
  def testTestAndSetList_NotConnected()
    key = "_TestAndSetList_NotConnected"
    conn = Scalaris::TransactionSingleOp.new()
    conn.close_connection()
    #assert_raise( Scalaris::ConnectionError ) { conn.test_and_set(@testTime.to_s + key, "fail", [$_TEST_DATA[0], $_TEST_DATA[1]]) }
    assert_raise( Scalaris::NotFoundError ) { conn.test_and_set(@testTime.to_s + key, "fail", [$_TEST_DATA[0], $_TEST_DATA[1]]) }
    conn.close_connection()
  end
  
  # Test method for TransactionSingleOp.test_and_set(key, oldvalue=str(), newvalue=list()).
  # Tries test_and_set with a non-existing key.
  def testTestAndSetList_NotFound()
    key = "_TestAndSetList_NotFound"
    conn = Scalaris::TransactionSingleOp.new()
    assert_raise( Scalaris::NotFoundError ) { conn.test_and_set(@testTime.to_s + key, "fail", [$_TEST_DATA[0], $_TEST_DATA[1]]) }
    conn.close_connection()
  end
  
  # Test method for TransactionSingleOp.test_and_set(key, oldvalue=str(), newvalue=list()),
  # TransactionSingleOp.read(key) and TransactionSingleOp.write(key, value=list()).
  # Writes a list and tries to overwrite it using test_and_set
  # knowing the correct old value. Tries to read the string afterwards.
  def testTestAndSetList1()
    key = "_TestAndSetList1"
    conn = Scalaris::TransactionSingleOp.new()
    
    # first write all values:
    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      conn.write(@testTime.to_s + key + i.to_s, [$_TEST_DATA[i], $_TEST_DATA[i + 1]])
    end
    
    # now try to overwrite them using test_and_set:
    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      conn.test_and_set(@testTime.to_s + key + i.to_s, [$_TEST_DATA[i], $_TEST_DATA[i + 1]], [$_TEST_DATA[i + 1], $_TEST_DATA[i]])
    end
    
    # now try to read the data:
    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      actual = conn.read(@testTime.to_s + key + i.to_s)
      assert_equal([$_TEST_DATA[i + 1], $_TEST_DATA[i]], actual)
    end
    
    conn.close_connection()
  end
  
  # Test method for TransactionSingleOp.test_and_set(key, oldvalue=str(), newvalue=list()),
  # TransactionSingleOp.read(key) and TransactionSingleOp.write(key, value=list()).
  # Writes a string and tries to overwrite it using test_and_set
  # knowing the wrong old value. Tries to read the string afterwards.
  def testTestAndSetList2()
    key = "_TestAndSetList2"
    conn = Scalaris::TransactionSingleOp.new()
    
    # first write all values:
    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      conn.write(@testTime.to_s + key + i.to_s, [$_TEST_DATA[i], $_TEST_DATA[i + 1]])
    end
    
    # now try to overwrite them using test_and_set:
    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      begin
        conn.test_and_set(@testTime.to_s + key + i.to_s, "fail", 1)
        assert(false, 'expected a KeyChangedError')
      rescue Scalaris::KeyChangedError => exception
        assert_equal([$_TEST_DATA[i], $_TEST_DATA[i + 1]], exception.old_value)
      end
    end
    
    # now try to read the data:
    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      actual = conn.read(@testTime.to_s + key + i.to_s)
      assert_equal([$_TEST_DATA[i], $_TEST_DATA[i + 1]], actual)
    end
    
    conn.close_connection()
  end

  # Test method for TransactionSingleOp.req_list(RequestList) with an
  # empty request list.
  def testReqList_Empty()
    conn = Scalaris::TransactionSingleOp.new()
    conn.req_list(conn.new_req_list())
    conn.close_connection()
  end
  
  # Test method for TransactionSingleOp.req_list(RequestList) with a
  # mixed request list.
  def testReqList1()
    key = "_ReqList1_"
    conn = Scalaris::TransactionSingleOp.new()
    
    readRequests = conn.new_req_list()
    firstWriteRequests = conn.new_req_list()
    writeRequests = conn.new_req_list()
    (0..($_TEST_DATA.length - 1)).each do |i|
      if (i % 2) == 0
        firstWriteRequests.add_write(@testTime.to_s + key + i.to_s, "first_" + $_TEST_DATA[i])
      end
      writeRequests.add_write(@testTime.to_s + key + i.to_s, "second_" + $_TEST_DATA[i])
      readRequests.add_read(@testTime.to_s + key + i.to_s)
    end
    
    results = conn.req_list(firstWriteRequests)
    # evaluate the first write results:
    (0..(firstWriteRequests.size() - 1)).step(2) do |i|
      conn.process_result_write(results[i])
    end

    results = conn.req_list(readRequests)
    assert_equal(readRequests.size(), results.length)
    # now evaluate the read results:
    (0..(readRequests.size() - 1)).step(2) do |i|
      if (i % 2) == 0
        actual = conn.process_result_read(results[i])
        assert_equal("first_" + $_TEST_DATA[i], actual)
      else
        begin
          conn.process_result_read(results[i])
          # a not found exception must be thrown
          assert(false, 'expected a NotFoundError')
        rescue Scalaris::NotFoundError
        end
      end
    end

    results = conn.req_list(writeRequests)
    assert_equal(writeRequests.size(), results.length)
    # now evaluate the write results:
    (0..(writeRequests.size() - 1)).step(2) do |i|
      conn.process_result_write(results[i])
    end

    # once again test reads - now all reads should be successful
    results = conn.req_list(readRequests)
    assert_equal(readRequests.size(), results.length)

    # now evaluate the read results:
    (0..(readRequests.size() - 1)).step(2) do |i|
      actual = conn.process_result_read(results[i])
      assert_equal("second_" + $_TEST_DATA[i], actual)
    end
    
    conn.close_connection();
  end

  # Test method for TransactionSingleOp.write(key, value=bytearray()) with a
  # request that is too large.
  def testReqTooLarge()
      conn = Scalaris::TransactionSingleOp.new()
      data = "0" * ($_TOO_LARGE_REQUEST_SIZE)
      key = "_ReqTooLarge"
      begin
        conn.write(@testTime.to_s + key, data)
        assert(false, 'The write should have failed unless yaws_max_post_data was set larger than ' + $_TOO_LARGE_REQUEST_SIZE.to_s())
      rescue Scalaris::ConnectionError
      end
      
      conn.close_connection()
  end
end

class TestTransaction < Test::Unit::TestCase
  def setup
    @testTime = (Time.now.to_f * 1000).to_i
  end
  
  # Test method for Transaction()
  def testTransaction1()
    t = Scalaris::Transaction.new()
    t.close_connection()
  end

  # Test method for Transaction(conn)
  def testTransaction3()
    t = Scalaris::Transaction.new(conn = Scalaris::JSONConnection.new(url = Scalaris::DEFAULT_URL))
    t.close_connection()
  end

  # Test method for Transaction.close_connection() trying to close the connection twice.
  def testDoubleClose()
    t = Scalaris::Transaction.new()
    t.close_connection()
    t.close_connection()
  end

  # Test method for Transaction.commit() with a closed connection.
  def testCommit_NotConnected()
    t = Scalaris::Transaction.new()
    t.close_connection()
    #assert_raise( Scalaris::ConnectionError ) { t.commit() }
    t.commit()
    t.close_connection()
  end

  # Test method for Transaction.commit() which commits an empty transaction.
  def testCommit_Empty()
    t = Scalaris::Transaction.new()
    t.commit()
    t.close_connection()
  end

  # Test method for Transaction.abort() with a closed connection.
  def testAbort_NotConnected()
    t = Scalaris::Transaction.new()
    t.close_connection()
    #assert_raise( Scalaris::ConnectionError ) { t.abort() }
    t.abort()
    t.close_connection()
  end

  # Test method for Transaction.abort() which aborts an empty transaction.
  def testAbort_Empty()
    t = Scalaris::Transaction.new()
    t.abort()
    t.close_connection()
  end

  # Test method for Transaction.read(key)
  def testRead_NotFound()
    key = "_Read_NotFound"
    t = Scalaris::Transaction.new()
    assert_raise( Scalaris::NotFoundError ) { t.read(@testTime.to_s + key) }
    t.close_connection()
  end

  # Test method for Transaction.read(key) with a closed connection.
  def testRead_NotConnected()
    key = "_Read_NotConnected"
    t = Scalaris::Transaction.new()
    t.close_connection()
    #assert_raise( Scalaris::ConnectionError ) { t.read(@testTime.to_s + key) }
    assert_raise( Scalaris::NotFoundError ) { t.read(@testTime.to_s + key) }
    t.close_connection()
  end

  # Test method for Transaction.write(key, value=str()) with a closed connection.
  def testWriteString_NotConnected()
    key = "_WriteString_NotConnected"
    t = Scalaris::Transaction.new()
    t.close_connection()
    #assert_raise( Scalaris::ConnectionError ) { t.write(@testTime.to_s + key, $_TEST_DATA[0]) }
    t.write(@testTime.to_s + key, $_TEST_DATA[0])
    t.close_connection()
  end

  # Test method for Transaction.read(key) and Transaction.write(key, value=str())
  # which should show that writing a value for a key for which a previous read
  # returned a NotFoundError is possible.
  def testWriteString_NotFound()
    key = "_WriteString_notFound"
    t = Scalaris::Transaction.new()
    notFound = false
    begin
      t.read(@testTime.to_s + key)
    rescue Scalaris::NotFoundError
      notFound = true
    end
    
    assert(notFound)
    t.write(@testTime.to_s + key, $_TEST_DATA[0])
    assert_equal($_TEST_DATA[0], t.read(@testTime.to_s + key))
    t.close_connection()
  end

  # Test method for Transaction.write(key, value=str()) and Transaction.read(key).
  # Writes strings and uses a distinct key for each value. Tries to read the data afterwards.
  def testWriteString()
    key = "_testWriteString1_"
    t = Scalaris::Transaction.new()

    (0..($_TEST_DATA.length - 1)).each do |i|
      t.write(@testTime.to_s + key + i.to_s, $_TEST_DATA[i])
    end
    
    # now try to read the data:
    (0..($_TEST_DATA.length - 1)).each do |i|
      actual = t.read(@testTime.to_s + key + i.to_s)
      assert_equal($_TEST_DATA[i], actual)
    end
    
    # commit the transaction and try to read the data with a new one:
    t.commit()
    t = Scalaris::Transaction.new()
    (0..($_TEST_DATA.length - 1)).each do |i|
      actual = t.read(@testTime.to_s + key + i.to_s)
      assert_equal($_TEST_DATA[i], actual)
    end
    
    t.close_connection()
  end

  # Test method for Transaction.write(key, value=list()) and Transaction.read(key).
  # Writes a list and uses a distinct key for each value. Tries to read the data afterwards.
  def testWriteList1()
    key = "_testWriteList1_"
    t = Scalaris::Transaction.new()

    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      t.write(@testTime.to_s + key + i.to_s, [$_TEST_DATA[i], $_TEST_DATA[i + 1]])
    end
    
    # now try to read the data:
    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      actual = t.read(@testTime.to_s + key + i.to_s)
      assert_equal([$_TEST_DATA[i], $_TEST_DATA[i + 1]], actual)
    end
    
    t.close_connection()
    
    # commit the transaction and try to read the data with a new one:
    t.commit()
    t = Scalaris::Transaction.new()
    (0..($_TEST_DATA.length - 2)).step(2) do |i|
      actual = t.read(@testTime.to_s + key + i.to_s)
      assert_equal([$_TEST_DATA[i], $_TEST_DATA[i + 1]], actual)
    end
    
    t.close_connection()
  end

  # Test method for Transaction.req_list(RequestList) with an
  # empty request list.
  def testReqList_Empty()
    conn = Scalaris::Transaction.new()
    conn.req_list(conn.new_req_list())
    conn.close_connection()
  end

  # Test method for Transaction.req_list(RequestList) with a
  # mixed request list.
  def testReqList1()
    key = "_ReqList1_"
    conn = Scalaris::Transaction.new()
    
    readRequests = conn.new_req_list()
    firstWriteRequests = conn.new_req_list()
    writeRequests = conn.new_req_list()
    (0..($_TEST_DATA.length - 1)).each do |i|
      if (i % 2) == 0
        firstWriteRequests.add_write(@testTime.to_s + key + i.to_s, $_TEST_DATA[i])
      end
      writeRequests.add_write(@testTime.to_s + key + i.to_s, $_TEST_DATA[i])
      readRequests.add_read(@testTime.to_s + key + i.to_s)
    end
    
    results = conn.req_list(firstWriteRequests)
    # evaluate the first write results:
    (0..(firstWriteRequests.size() - 1)).each do |i|
      conn.process_result_write(results[i])
    end

    requests = conn.new_req_list(readRequests).concat(writeRequests).add_commit()
    results = conn.req_list(requests)
    assert_equal(requests.size(), results.length)

    # now evaluate the read results:
    (0..(readRequests.size() - 1)).each do |i|
      if (i % 2) == 0
        actual = conn.process_result_read(results[i])
        assert_equal($_TEST_DATA[i], actual)
      else
        begin
          conn.process_result_read(results[i])
          # a not found exception must be thrown
          assert(false, 'expected a NotFoundError')
        rescue Scalaris::NotFoundError
        end
      end
    end

    # now evaluate the write results:
    (0..(writeRequests.size() - 1)).each do |i|
      pos = readRequests.size() + i
      conn.process_result_write(results[pos])
    end

    # once again test reads - now all reads should be successful
    results = conn.req_list(readRequests)
    assert_equal(readRequests.size(), results.length)

    # now evaluate the read results:
    (0..(readRequests.size() - 1)).each do |i|
      actual = conn.process_result_read(results[i])
      assert_equal($_TEST_DATA[i], actual)
    end
    
    conn.close_connection();
  end

  # Test method for Transaction.write(key, value=bytearray()) with a
  # request that is too large.
  def testReqTooLarge()
      conn = Scalaris::Transaction.new()
      data = "0" * ($_TOO_LARGE_REQUEST_SIZE)
      key = "_ReqTooLarge"
      begin
        conn.write(@testTime.to_s + key, data)
        assert(false, 'The write should have failed unless yaws_max_post_data was set larger than ' + $_TOO_LARGE_REQUEST_SIZE.to_s())
      rescue Scalaris::ConnectionError
      end
      
      conn.close_connection()
  end

  # Various tests.
  def testVarious()
      _writeSingleTest("_0:" + [0x0160].pack("U*") + "arplaninac:page_", $_TEST_DATA[0])
  end
  
  # Helper function for single write tests.
  # Writes a strings to some key and tries to read it afterwards.
  def _writeSingleTest(key, data)
    t = Scalaris::Transaction.new()
  
    t.write(@testTime.to_s + key, data)
    # now try to read the data:
    assert_equal(data, t.read(@testTime.to_s + key))
    # commit the transaction and try to read the data with a new one:
    t.commit()
    t = Scalaris::Transaction.new()
    assert_equal(data, t.read(@testTime.to_s + key))
    
    t.close_connection()
  end
  private :_writeSingleTest
end

class TestReplicatedDHT < Test::Unit::TestCase
  def setup
    @testTime = (Time.now.to_f * 1000).to_i
  end
  
  # Test method for ReplicatedDHT()
  def testReplicatedDHT1()
    rdht = Scalaris::ReplicatedDHT.new()
    rdht.close_connection()
  end

  # Test method for ReplicatedDHT(conn)
  def testReplicatedDHT2()
    rdht = Scalaris::ReplicatedDHT.new(conn = Scalaris::JSONConnection.new(url = Scalaris::DEFAULT_URL))
    rdht.close_connection()
  end

  # Test method for ReplicatedDHT.close_connection() trying to close the connection twice.
  def testDoubleClose()
    rdht = Scalaris::ReplicatedDHT.new()
    rdht.close_connection()
    rdht.close_connection()
  end
  
  # Tries to read the value at the given key and fails if this does
  # not fail with a NotFoundError.
  def _checkKeyDoesNotExist(key)
    conn = Scalaris::TransactionSingleOp.new()
    begin
      conn.read(key)
      assert(false, 'the value at ' + key + ' should not exist anymore')
    rescue Scalaris::NotFoundError
      # nothing to do here
    end
    conn.close_connection()
  end

  # Test method for ReplicatedDHT.delete(key).
  # Tries to delete some not existing keys.
  def testDelete_notExistingKey()
    key = "_Delete_NotExistingKey"
    rdht = Scalaris::ReplicatedDHT.new()
    rt = Scalaris::RoutingTable.new
    r = rt.get_replication_factor

    (0..($_TEST_DATA.length - 1)).each do |i|
      ok = rdht.delete(@testTime.to_s + key + i.to_s)
      assert_equal(0, ok)
      results = rdht.get_last_delete_result()
      assert_equal(0, results.ok)
      assert_equal(0, results.locks_set)
      assert_equal(r, results.undefined)
      _checkKeyDoesNotExist(@testTime.to_s + key + i.to_s)
    end
    
    rdht.close_connection()
  end

  # Test method for ReplicatedDHT.delete(key) and TransactionSingleOp#write(key, value=str()).
  # Inserts some values, tries to delete them afterwards and tries the delete again.
  def testDelete1()
    key = "_Delete1"
    c = Scalaris::JSONConnection.new(url = Scalaris::DEFAULT_URL)
    rdht = Scalaris::ReplicatedDHT.new(conn = c)
    sc = Scalaris::TransactionSingleOp.new(conn = c)
    rt = Scalaris::RoutingTable.new
    r = rt.get_replication_factor
    
    (0..($_TEST_DATA.length - 1)).each do |i|
      sc.write(@testTime.to_s + key + i.to_s, $_TEST_DATA[i])
    end
    
    # now try to delete the data:
    (0..($_TEST_DATA.length - 1)).each do |i|
      ok = rdht.delete(@testTime.to_s + key + i.to_s)
      assert_equal(r, ok)
      results = rdht.get_last_delete_result()
      assert_equal(r, results.ok)
      assert_equal(0, results.locks_set)
      assert_equal(0, results.undefined)
      _checkKeyDoesNotExist(@testTime.to_s + key + i.to_s)
      
      # try again (should be successful with 0 deletes)
      ok = rdht.delete(@testTime.to_s + key + i.to_s)
      assert_equal(0, ok)
      results = rdht.get_last_delete_result()
      assert_equal(0, results.ok)
      assert_equal(0, results.locks_set)
      assert_equal(r, results.undefined)
      _checkKeyDoesNotExist(@testTime.to_s + key + i.to_s)
    end
    
    c.close()
  end

  # Test method for ReplicatedDHT.delete(key) and TransactionSingleOp#write(key, value=str()).
  # Inserts some values, tries to delete them afterwards, inserts them again and tries to delete them again (twice).
  def testDelete2()
    key = "_Delete2"
    c = Scalaris::JSONConnection.new(url = Scalaris::DEFAULT_URL)
    rdht = Scalaris::ReplicatedDHT.new(conn = c)
    sc = Scalaris::TransactionSingleOp.new(conn = c)
    rt = Scalaris::RoutingTable.new
    r = rt.get_replication_factor

    (0..($_TEST_DATA.length - 1)).each do |i|
      sc.write(@testTime.to_s + key + i.to_s, $_TEST_DATA[i])
    end
    
    # now try to delete the data:
    (0..($_TEST_DATA.length - 1)).each do |i|
      ok = rdht.delete(@testTime.to_s + key + i.to_s)
      assert_equal(r, ok)
      results = rdht.get_last_delete_result()
      assert_equal(r, results.ok)
      assert_equal(0, results.locks_set)
      assert_equal(0, results.undefined)
      _checkKeyDoesNotExist(@testTime.to_s + key + i.to_s)
    end
  
    (0..($_TEST_DATA.length - 1)).each do |i|
      sc.write(@testTime.to_s + key + i.to_s, $_TEST_DATA[i])
    end
    
    # now try to delete the data:
    (0..($_TEST_DATA.length - 1)).each do |i|
      ok = rdht.delete(@testTime.to_s + key + i.to_s)
      assert_equal(r, ok)
      results = rdht.get_last_delete_result()
      assert_equal(r, results.ok)
      assert_equal(0, results.locks_set)
      assert_equal(0, results.undefined)
      _checkKeyDoesNotExist(@testTime.to_s + key + i.to_s)
      
      # try again (should be successful with 0 deletes)
      ok = rdht.delete(@testTime.to_s + key + i.to_s)
      assert_equal(0, ok)
      results = rdht.get_last_delete_result()
      assert_equal(0, results.ok)
      assert_equal(0, results.locks_set)
      assert_equal(r, results.undefined)
      _checkKeyDoesNotExist(@testTime.to_s + key + i.to_s)
    end
    
    c.close()
  end
end
