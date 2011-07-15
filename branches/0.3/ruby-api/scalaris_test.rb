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
  require "#{File.dirname(__FILE__)}/scalaris"
rescue LoadError
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
        firstWriteRequests.add_write(@testTime.to_s + key + i.to_s, $_TEST_DATA[i])
      end
      writeRequests.add_write(@testTime.to_s + key + i.to_s, $_TEST_DATA[i])
      readRequests.add_read(@testTime.to_s + key + i.to_s)
    end
    
    results = conn.req_list(firstWriteRequests)
    # evaluate the first write results:
    (0..(firstWriteRequests.size() - 1)).step(2) do |i|
      conn.process_result_write(results[i])
    end

    requests = conn.new_req_list(readRequests).concat(writeRequests)
    results = conn.req_list(requests)
    assert_equal(requests.size(), results.length)

    # now evaluate the read results:
    (0..(readRequests.size() - 1)).step(2) do |i|
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
    (0..(writeRequests.size() - 1)).step(2) do |i|
      pos = readRequests.size() + i
      conn.process_result_write(results[pos])
    end

    # once again test reads - now all reads should be successful
    results = conn.req_list(readRequests)
    assert_equal(readRequests.size(), results.length)

    # now evaluate the read results:
    (0..(readRequests.size() - 1)).step(2) do |i|
      actual = conn.process_result_read(results[i])
      assert_equal($_TEST_DATA[i], actual)
    end
    
    conn.close_connection();
  end

  # Test method for TransactionSingleOp.write(key, value=bytearray()) with a
  # request that is too large.
  def testReqTooLarge()
      conn = Scalaris::TransactionSingleOp.new()
      data = (0..($_TOO_LARGE_REQUEST_SIZE)).map{0}.join()
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
      data = (0..($_TOO_LARGE_REQUEST_SIZE)).map{0}.join()
      key = "_ReqTooLarge"
      begin
        conn.write(@testTime.to_s + key, data)
        assert(false, 'The write should have failed unless yaws_max_post_data was set larger than ' + $_TOO_LARGE_REQUEST_SIZE.to_s())
      rescue Scalaris::ConnectionError
      end
      
      conn.close_connection()
  end
end

  class TestPubSub < Test::Unit::TestCase
    def setup
      @testTime = (Time.now.to_f * 1000).to_i
    end
    
    # checks if there are more elements in list than in expectedElements and returns one of those elements
    def self._getDiffElement(list, expectedElements)
      for e in expectedElements:
        list.delete(e)
      end
      
      if list.length > 0
        return list[0]
      else
        return nil
      end
    end
  
    # Test method for PubSub()
    def testPubSub1()
      conn = Scalaris::PubSub.new()
      conn.close_connection()
    end
  
    # Test method for PubSub(conn)
    def testPubSub2()
      conn = Scalaris::PubSub.new(conn = Scalaris::JSONConnection.new(url = Scalaris::DEFAULT_URL))
      conn.close_connection()
    end
  
    # Test method for PubSub.close_connection() trying to close the connection twice.
    def testDoubleClose()
      conn = Scalaris::PubSub.new()
      conn.close_connection()
      conn.close_connection()
    end
  
    # Test method for PubSub.publish(topic, content) with a closed connection.
    def testPublish_NotConnected()
      topic = "_Publish_NotConnected"
      conn = Scalaris::PubSub.new()
      conn.close_connection()
      #assert_raise( Scalaris::ConnectionError ) { conn.publish(@testTime.to_s + topic, $_TEST_DATA[0]) }
      conn.publish(@testTime.to_s + topic, $_TEST_DATA[0])
      conn.close_connection()
    end
  
    # Test method for PubSub.publish(topic, content).
    # Publishes some topics and uses a distinct key for each value.
    def testPublish1()
      topic = "_Publish1_"
      conn = Scalaris::PubSub.new()
  
      (0..($_TEST_DATA.length - 1)).each do |i|
        conn.publish(@testTime.to_s + topic + i.to_s, $_TEST_DATA[i])
      end
      
      conn.close_connection()
    end

    # Test method for PubSub.publish(topic, content).
    # Publishes some topics and uses a single key for all the values.
    def testPublish2()
      topic = "_Publish2"
      conn = Scalaris::PubSub.new()
  
      (0..($_TEST_DATA.length - 1)).each do |i|
        conn.publish(@testTime.to_s + topic, $_TEST_DATA[i])
      end
      
      conn.close_connection()
    end

    # Test method for PubSub.get_subscribers(topic) with a closed connection.
    def testGetSubscribersOtp_NotConnected()
      topic = "_GetSubscribers_NotConnected"
      conn = Scalaris::PubSub.new()
      conn.close_connection()
      #assert_raise( Scalaris::ConnectionError ) { conn.get_subscribers(@testTime.to_s + topic) }
      conn.get_subscribers(@testTime.to_s + topic)
      conn.close_connection()
    end

    # Test method for PubSub.get_subscribers(topic).
    # Tries to get a subscriber list from an empty topic.
    def testGetSubscribers_NotExistingTopic()
      topic = "_GetSubscribers_NotExistingTopic"
      conn = Scalaris::PubSub.new()
      subscribers = conn.get_subscribers(@testTime.to_s + topic)
      assert_equal([], subscribers)
      conn.close_connection()
    end

    # Test method for PubSub.subscribe(topic url) with a closed connection.
    def testSubscribe_NotConnected()
      topic = "_Subscribe_NotConnected"
      conn = Scalaris::PubSub.new()
      conn.close_connection()
      #assert_raise( Scalaris::ConnectionError ) { conn.subscribe(@testTime.to_s + topic, $_TEST_DATA[0]) }
      conn.subscribe(@testTime.to_s + topic, $_TEST_DATA[0])
      conn.close_connection()
    end

    # Test method for PubSub.subscribe(topic, url) and PubSub.get_subscribers(topic).
    # Subscribes some arbitrary URLs to arbitrary topics and uses a distinct topic for each URL.
    def testSubscribe1()
      topic = "_Subscribe1_"
      conn = Scalaris::PubSub.new()
  
      (0..($_TEST_DATA.length - 1)).each do |i|
        conn.subscribe(@testTime.to_s + topic + i.to_s, $_TEST_DATA[i])
      end
      
      # check if the subscribers were successfully saved:
      (0..($_TEST_DATA.length - 1)).each do |i|
        topic1 = topic + i.to_s
        subscribers = conn.get_subscribers(@testTime.to_s + topic1)
        assert(subscribers.include?($_TEST_DATA[i]),
               "Subscriber \"" + $_TEST_DATA[i] + "\" does not exist for topic \"" + topic1 + "\"")
        assert_equal(1, subscribers.length,
                     "Subscribers of topic (" + topic1 + ") should only be [" + $_TEST_DATA[i] + "], but is: " + subscribers.to_s)
      end
      
      conn.close_connection()
    end

    # Test method for PubSub.subscribe(topic, url) and PubSub.get_subscribers(topic).
    # Subscribes some arbitrary URLs to arbitrary topics and uses a single topic for all URLs.
    def testSubscribe2()
      topic = "_Subscribe2"
      conn = Scalaris::PubSub.new()

      (0..($_TEST_DATA.length - 1)).each do |i|
        conn.subscribe(@testTime.to_s + topic, $_TEST_DATA[i])
      end
      
      # check if the subscribers were successfully saved:
      subscribers = conn.get_subscribers(@testTime.to_s + topic)
      (0..($_TEST_DATA.length - 1)).each do |i|
        assert(subscribers.include?($_TEST_DATA[i]),
               "Subscriber \"" + $_TEST_DATA[i] + "\" does not exist for topic \"" + topic + "\"")
      end
      assert_equal(nil, self.class._getDiffElement(subscribers, $_TEST_DATA),
                   "unexpected subscriber of topic \"" + topic + "\"")
      
      conn.close_connection()
    end

    # Test method for PubSub.unsubscribe(topic url) with a closed connection.
    def testUnsubscribe_NotConnected()
      topic = "_Unsubscribe_NotConnected"
      conn = Scalaris::PubSub.new()
      conn.close_connection()
      #assert_raise( Scalaris::ConnectionError ) { conn.unsubscribe(@testTime.to_s + topic, $_TEST_DATA[0]) }
      assert_raise( Scalaris::NotFoundError ) { conn.unsubscribe(@testTime.to_s + topic, $_TEST_DATA[0]) }
      conn.close_connection()
    end

    # Test method for PubSub.unsubscribe(topic url) and PubSub.get_subscribers(topic).
    # Tries to unsubscribe an URL from a non-existing topic and tries to get the subscriber list afterwards.
    def testUnsubscribe_NotExistingTopic()
      topic = "_Unsubscribe_NotExistingTopic"
      conn = Scalaris::PubSub.new()
      # unsubscribe test "url":
      assert_raise( Scalaris::NotFoundError ) { conn.unsubscribe(@testTime.to_s + topic, $_TEST_DATA[0]) }
      
      # check whether the unsubscribed urls were unsubscribed:
      subscribers = conn.get_subscribers(@testTime.to_s + topic)
      assert(!(subscribers.include?($_TEST_DATA[0])),
             "Subscriber \"" + $_TEST_DATA[0] + "\" should have been unsubscribed from topic \"" + topic + "\"")
      assert_equal(0, subscribers.length,
                   "Subscribers of topic (" + topic + ") should only be [], but is: " + subscribers.to_s)
      
      conn.close_connection()
    end

    # Test method for PubSub.subscribe(topic url), PubSub.unsubscribe(topic url) and PubSub.get_subscribers(topic).
    # Tries to unsubscribe an unsubscribed URL from an existing topic and compares the subscriber list afterwards.
    def testUnsubscribe_NotExistingUrl()
      topic = "_Unsubscribe_NotExistingUrl"
      conn = Scalaris::PubSub.new()
      
      # first subscribe test "urls"...
      conn.subscribe(@testTime.to_s + topic, $_TEST_DATA[0])
      conn.subscribe(@testTime.to_s + topic, $_TEST_DATA[1])
      
      # then unsubscribe another "url":
      assert_raise( Scalaris::NotFoundError ) { conn.unsubscribe(@testTime.to_s + topic, $_TEST_DATA[2]) }
      
      # check whether the subscribers were successfully saved:
      subscribers = conn.get_subscribers(@testTime.to_s + topic)
      assert(subscribers.include?($_TEST_DATA[0]),
             "Subscriber \"" + $_TEST_DATA[0] + "\" does not exist for topic \"" + topic + "\"")
      assert(subscribers.include?($_TEST_DATA[1]),
             "Subscriber \"" + $_TEST_DATA[1] + "\" does not exist for topic \"" + topic + "\"")
      
      # check whether the unsubscribed urls were unsubscribed:
      assert(!(subscribers.include?($_TEST_DATA[2])),
             "Subscriber \"" + $_TEST_DATA[2] + "\" should have been unsubscribed from topic \"" + topic + "\"")
      
      assert_equal(2, subscribers.length,
                   "Subscribers of topic (" + topic + ") should only be [\"" + $_TEST_DATA[0] + "\", \"" + $_TEST_DATA[1] + "\"], but is: " + subscribers.to_s)
      
      conn.close_connection()
    end

    # Test method for PubSub.subscribe(topic url), PubSub.unsubscribe(topic url) and PubSub.get_subscribers(topic).
    # Subscribes some arbitrary URLs to arbitrary topics and uses a distinct topic for each URL.
    # Unsubscribes every second subscribed URL.
    def testUnsubscribe1()
      topic = "_UnsubscribeString1_"
      conn = Scalaris::PubSub.new()
      
      # first subscribe test "urls"...
      (0..($_TEST_DATA.length - 1)).each do |i|
        conn.subscribe(@testTime.to_s + topic + i.to_s, $_TEST_DATA[i])
      end
      
      # ... then unsubscribe every second url:
      (0..($_TEST_DATA.length - 1)).step(2) do |i|
        conn.unsubscribe(@testTime.to_s + topic + i.to_s, $_TEST_DATA[i])
      end
      
      # check whether the subscribers were successfully saved:
      (1..($_TEST_DATA.length - 1)).step(2) do |i|
        topic1 = topic + i.to_s
        subscribers = conn.get_subscribers(@testTime.to_s + topic1)
        assert(subscribers.include?($_TEST_DATA[i]),
               "Subscriber \"" + $_TEST_DATA[i] + "\" does not exist for topic \"" + topic1 + "\"")
        assert_equal(1, subscribers.length,
                     "Subscribers of topic (" + topic1 + ") should only be [\"" + $_TEST_DATA[i] + "\"], but is: " + subscribers.to_s)
      end
      
      # check whether the unsubscribed urls were unsubscribed:
      (0..($_TEST_DATA.length - 1)).step(2) do |i|
        topic1 = topic + i.to_s
        subscribers = conn.get_subscribers(@testTime.to_s + topic1)
        assert(!(subscribers.include?($_TEST_DATA[i])),
               "Subscriber \"" + $_TEST_DATA[i] + "\" should have been unsubscribed from topic \"" + topic1 + "\"")
        assert_equal(0, subscribers.length,
                     "Subscribers of topic (" + topic1 + ") should only be [], but is: " + subscribers.to_s)
      end
      
      conn.close_connection()
    end

    # Test method for PubSub.subscribe(topic url), PubSub.unsubscribe(topic url) and PubSub.get_subscribers(topic).
    # Subscribes some arbitrary URLs to arbitrary topics and uses a single topic for all URLs.
    # Unsubscribes every second subscribed URL.
    def testUnsubscribe2()
      topic = "_UnubscribeString2"
      conn = Scalaris::PubSub.new()
      
      # first subscribe all test "urls"...
      (0..($_TEST_DATA.length - 1)).each do |i|
        conn.subscribe(@testTime.to_s + topic, $_TEST_DATA[i])
      end
      
      # ... then unsubscribe every second url:
      (0..($_TEST_DATA.length - 1)).step(2) do |i|
        conn.unsubscribe(@testTime.to_s + topic, $_TEST_DATA[i])
      end
      
      # check whether the subscribers were successfully saved:
      subscribers = conn.get_subscribers(@testTime.to_s + topic)
      subscribers_expected = []
      (1..($_TEST_DATA.length - 1)).step(2) do |i|
        subscribers_expected << $_TEST_DATA[i]
        assert(subscribers.include?($_TEST_DATA[i]),
               "Subscriber \"" + $_TEST_DATA[i] + "\" does not exist for topic \"" + topic + "\"")
      end
      
      # check whether the unsubscribed urls were unsubscribed:
      (0..($_TEST_DATA.length - 1)).step(2) do |i|
        assert(!(subscribers.include?($_TEST_DATA[i])),
               "Subscriber \"" + $_TEST_DATA[i] + "\" should have been unsubscribed from topic \"" + topic + "\"")
      end
      
      assert_equal(nil, self.class._getDiffElement(subscribers, subscribers_expected),
                   "unexpected subscriber of topic \"" + topic + "\"")
                      
      conn.close_connection()
    end

#    def _checkNotifications(self, notifications, expected):
#        for (topic, contents) in expected.items():
#            if topic not in notifications:
#                notifications[topic] = []
#            for content in contents:
#                self.assertTrue(content in notifications[topic],
#                                msg = "subscription (" + topic + ", " + content + ") not received by server)")
#                notifications[topic].remove(content)
#            if len(notifications[topic]) > 0:
#                self.fail("Received element (" + topic + ", " + notifications[topic][0] + ") which is not part of the subscription.")
#            del notifications[topic]
#        # is there another (unexpected) topic we received content for?
#        if len(notifications) > 0:
#            for (topic, contents) in notifications.items():
#                if len(contents) > 0:
#                    self.fail("Received notification for topic (" + topic + ", " + contents[0] + ") which is not part of the subscription.")
#                    break
#    
#    # Test method for the publish/subscribe system.
#    # Single server, subscription to one topic, multiple publishs.
#    def testSubscription1()
#        topic = @testTime.to_s + "_Subscription1"
#        conn = Scalaris::PubSub.new()
#        server1 = self._newSubscriptionServer()
#        notifications_server1_expected = {topic: []}
#        ip1, port1 = server1.server_address
#        
#        conn.subscribe(topic, 'http://' + str(ip1) + ':' + str(port1))
#        for i in xrange(len(_TEST_DATA)):
#            conn.publish(topic, $_TEST_DATA[i])
#            notifications_server1_expected[topic].append($_TEST_DATA[i])
#        
#        # wait max '_NOTIFICATIONS_TIMEOUT' seconds for notifications:
#        for i in xrange(_NOTIFICATIONS_TIMEOUT):
#            if topic not in server1.notifications or len(server1.notifications[topic]) < len(notifications_server1_expected[topic]):
#                time.sleep(1)
#            else:
#                break
#        
#        server1.shutdown()
#        
#        # check that every notification arrived:
#        self._checkNotifications(server1.notifications, notifications_server1_expected)
#        conn.close_connection()
#    
#    # Test method for the publish/subscribe system.
#    # Three servers, subscription to one topic, multiple publishs.
#    def testSubscription2()
#        topic = @testTime.to_s + "_Subscription2"
#        conn = Scalaris::PubSub.new()
#        server1 = self._newSubscriptionServer()
#        server2 = self._newSubscriptionServer()
#        server3 = self._newSubscriptionServer()
#        notifications_server1_expected = {topic: []}
#        notifications_server2_expected = {topic: []}
#        notifications_server3_expected = {topic: []}
#        ip1, port1 = server1.server_address
#        ip2, port2 = server2.server_address
#        ip3, port3 = server3.server_address
#        
#        conn.subscribe(topic, 'http://' + str(ip1) + ':' + str(port1))
#        conn.subscribe(topic, 'http://' + str(ip2) + ':' + str(port2))
#        conn.subscribe(topic, 'http://' + str(ip3) + ':' + str(port3))
#        for i in xrange(len(_TEST_DATA)):
#            conn.publish(topic, $_TEST_DATA[i])
#            notifications_server1_expected[topic].append($_TEST_DATA[i])
#            notifications_server2_expected[topic].append($_TEST_DATA[i])
#            notifications_server3_expected[topic].append($_TEST_DATA[i])
#        
#        # wait max '_NOTIFICATIONS_TIMEOUT' seconds for notifications:
#        for i in xrange(_NOTIFICATIONS_TIMEOUT):
#            if (topic not in server1.notifications or len(server1.notifications[topic]) < len(notifications_server1_expected[topic])) or \
#               (topic not in server2.notifications or len(server2.notifications[topic]) < len(notifications_server2_expected[topic])) or \
#               (topic not in server3.notifications or len(server3.notifications[topic]) < len(notifications_server3_expected[topic])):
#                time.sleep(1)
#            else:
#                break
#        
#        server1.shutdown()
#        server2.shutdown()
#        server3.shutdown()
#        
#        # check that every notification arrived:
#        self._checkNotifications(server1.notifications, notifications_server1_expected)
#        self._checkNotifications(server2.notifications, notifications_server2_expected)
#        self._checkNotifications(server3.notifications, notifications_server3_expected)
#        conn.close_connection()
#    
#    # Test method for the publish/subscribe system.
#    # Three servers, subscription to different topics, multiple publishs, each
#    # server receives a different number of elements.
#    def testSubscription3()
#        topic1 = @testTime.to_s + "_Subscription3_1"
#        topic2 = @testTime.to_s + "_Subscription3_2"
#        topic3 = @testTime.to_s + "_Subscription3_3"
#        conn = Scalaris::PubSub.new()
#        server1 = self._newSubscriptionServer()
#        server2 = self._newSubscriptionServer()
#        server3 = self._newSubscriptionServer()
#        notifications_server1_expected = {topic1: []}
#        notifications_server2_expected = {topic2: []}
#        notifications_server3_expected = {topic3: []}
#        ip1, port1 = server1.server_address
#        ip2, port2 = server2.server_address
#        ip3, port3 = server3.server_address
#        
#        conn.subscribe(topic1, 'http://' + str(ip1) + ':' + str(port1))
#        conn.subscribe(topic2, 'http://' + str(ip2) + ':' + str(port2))
#        conn.subscribe(topic3, 'http://' + str(ip3) + ':' + str(port3))
#        for i in xrange(0, len(_TEST_DATA), 2):
#            conn.publish(topic1, $_TEST_DATA[i])
#            notifications_server1_expected[topic1].append($_TEST_DATA[i])
#        for i in xrange(0, len(_TEST_DATA), 3):
#            conn.publish(topic2, $_TEST_DATA[i])
#            notifications_server2_expected[topic2].append($_TEST_DATA[i])
#        for i in xrange(0, len(_TEST_DATA), 5):
#            conn.publish(topic3, $_TEST_DATA[i])
#            notifications_server3_expected[topic3].append($_TEST_DATA[i])
#        
#        # wait max '_NOTIFICATIONS_TIMEOUT' seconds for notifications:
#        for i in xrange(_NOTIFICATIONS_TIMEOUT):
#            if (topic1 not in server1.notifications or len(server1.notifications[topic1]) < len(notifications_server1_expected[topic1])) or \
#               (topic2 not in server2.notifications or len(server2.notifications[topic2]) < len(notifications_server2_expected[topic2])) or \
#               (topic3 not in server3.notifications or len(server3.notifications[topic3]) < len(notifications_server3_expected[topic3])):
#                time.sleep(1)
#            else:
#                break
#        
#        server1.shutdown()
#        server2.shutdown()
#        server3.shutdown()
#        
#        # check that every notification arrived:
#        self._checkNotifications(server1.notifications, notifications_server1_expected)
#        self._checkNotifications(server2.notifications, notifications_server2_expected)
#        self._checkNotifications(server3.notifications, notifications_server3_expected)
#        conn.close_connection()
#    
#    # Test method for the publish/subscribe system.
#    # Like testSubscription3() but some subscribed urls will be unsubscribed.
#    def testSubscription4()
#        topic1 = @testTime.to_s + "_Subscription4_1"
#        topic2 = @testTime.to_s + "_Subscription4_2"
#        topic3 = @testTime.to_s + "_Subscription4_3"
#        conn = Scalaris::PubSub.new()
#        server1 = self._newSubscriptionServer()
#        server2 = self._newSubscriptionServer()
#        server3 = self._newSubscriptionServer()
#        notifications_server1_expected = {topic1: []}
#        notifications_server2_expected = {topic2: []}
#        notifications_server3_expected = {topic3: []}
#        ip1, port1 = server1.server_address
#        ip2, port2 = server2.server_address
#        ip3, port3 = server3.server_address
#        
#        conn.subscribe(topic1, 'http://' + str(ip1) + ':' + str(port1))
#        conn.subscribe(topic2, 'http://' + str(ip2) + ':' + str(port2))
#        conn.subscribe(topic3, 'http://' + str(ip3) + ':' + str(port3))
#        conn.unsubscribe(topic2, 'http://' + str(ip2) + ':' + str(port2))
#        for i in xrange(0, len(_TEST_DATA), 2):
#            conn.publish(topic1, $_TEST_DATA[i])
#            notifications_server1_expected[topic1].append($_TEST_DATA[i])
#        for i in xrange(0, len(_TEST_DATA), 3):
#            conn.publish(topic2, $_TEST_DATA[i])
#            # note: topic2 is unsubscribed
#            # notifications_server2_expected[topic2].append($_TEST_DATA[i])
#        for i in xrange(0, len(_TEST_DATA), 5):
#            conn.publish(topic3, $_TEST_DATA[i])
#            notifications_server3_expected[topic3].append($_TEST_DATA[i])
#        
#        # wait max '_NOTIFICATIONS_TIMEOUT' seconds for notifications:
#        for i in xrange(_NOTIFICATIONS_TIMEOUT):
#            if (topic1 not in server1.notifications or len(server1.notifications[topic1]) < len(notifications_server1_expected[topic1])) or \
#               (topic3 not in server3.notifications or len(server3.notifications[topic3]) < len(notifications_server3_expected[topic3])):
#                time.sleep(1)
#            else:
#                break
#        
#        server1.shutdown()
#        server2.shutdown()
#        server3.shutdown()
#        
#        # check that every notification arrived:
#        self._checkNotifications(server1.notifications, notifications_server1_expected)
#        self._checkNotifications(server2.notifications, notifications_server2_expected)
#        self._checkNotifications(server3.notifications, notifications_server3_expected)
#        conn.close_connection()
#
#    @staticmethod
#    def _newSubscriptionServer(server_address = ('localhost', 0)):
#        server = TestPubSub.SubscriptionServer(server_address)
#        #ip, port = server.server_address
#    
#        # Start a thread with the server
#        server_thread = threading.Thread(target=server.serve_forever)
#        # Exit the server thread when the main thread terminates
#        server_thread.setDaemon(True)
#        server_thread.start()
#        #print "Server loop running in thread:", server_thread.getName()
#    
#        return server
#    
#    class SubscriptionServer(HTTPServer):
#        def __init__(self, server_address):
#            HTTPServer.__init__(self, server_address, TestPubSub.SubscriptionHandler)
#            self.notifications = {}
#    
#    class SubscriptionHandler(BaseHTTPRequestHandler):
#        def do_POST()
#            if 'content-length' in self.headers and 'content-type' in self.headers:
#                length = int(self.headers['content-length'])
#                charset = self.headers['content-type'].split('charset=')
#                if (len(charset) > 1):
#                    encoding = charset[-1]
#                else:
#                    encoding = 'utf-8'
#                data = self.rfile.read(length).decode(encoding)
#                response_json = json.loads(data)
#                # {"method":"notify","params":["1209386211287_SubscribeTest","content"],"id":482975}
#                if 'method' in response_json and response_json['method'] == 'notify' \
#                    and 'params' in response_json and 'id' in response_json \
#                    and isinstance(response_json['params'], list) and len(response_json['params']) == 2:
#                        topic = response_json['params'][0]
#                        content = response_json['params'][1]
#                        if hasattr(self.server, 'notifications'):
#                            if topic not in self.server.notifications:
#                                self.server.notifications[topic] = []
#                            self.server.notifications[topic].append(content)
#            else:
#                pass
#            
#            response = '{}'.encode('utf-8')
#            self.send_response(200)
#            self.send_header("Content-type", "text/html; charset=utf-8")
#            self.send_header("Content-length", str(len(response)))
#            self.end_headers()
#            self.wfile.write(response)
#        
#        # to disable logging
#        def log_message(self, *args):
#            pass

  # Test method for PubSub.write(key, value=bytearray()) with a
  # request that is too large.
  def testReqTooLarge()
      conn = Scalaris::PubSub.new()
      data = (0..($_TOO_LARGE_REQUEST_SIZE)).map{0}.join()
      key = "_ReqTooLarge"
      begin
        conn.publish(@testTime.to_s + key, data)
        assert(false, 'The publish should have failed unless yaws_max_post_data was set larger than ' + $_TOO_LARGE_REQUEST_SIZE.to_s())
      rescue Scalaris::ConnectionError
      end
      
      conn.close_connection()
  end
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

    (0..($_TEST_DATA.length - 1)).each do |i|
      ok = rdht.delete(@testTime.to_s + key + i.to_s)
      assert_equal(0, ok)
      results = rdht.get_last_delete_result()
      assert_equal(0, results.ok)
      assert_equal(0, results.locks_set)
      assert_equal(4, results.undefined)
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

    (0..($_TEST_DATA.length - 1)).each do |i|
      sc.write(@testTime.to_s + key + i.to_s, $_TEST_DATA[i])
    end
    
    # now try to delete the data:
    (0..($_TEST_DATA.length - 1)).each do |i|
      ok = rdht.delete(@testTime.to_s + key + i.to_s)
      assert_equal(4, ok)
      results = rdht.get_last_delete_result()
      assert_equal(4, results.ok)
      assert_equal(0, results.locks_set)
      assert_equal(0, results.undefined)
      _checkKeyDoesNotExist(@testTime.to_s + key + i.to_s)
      
      # try again (should be successful with 0 deletes)
      ok = rdht.delete(@testTime.to_s + key + i.to_s)
      assert_equal(0, ok)
      results = rdht.get_last_delete_result()
      assert_equal(0, results.ok)
      assert_equal(0, results.locks_set)
      assert_equal(4, results.undefined)
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

    (0..($_TEST_DATA.length - 1)).each do |i|
      sc.write(@testTime.to_s + key + i.to_s, $_TEST_DATA[i])
    end
    
    # now try to delete the data:
    (0..($_TEST_DATA.length - 1)).each do |i|
      ok = rdht.delete(@testTime.to_s + key + i.to_s)
      assert_equal(4, ok)
      results = rdht.get_last_delete_result()
      assert_equal(4, results.ok)
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
      assert_equal(4, ok)
      results = rdht.get_last_delete_result()
      assert_equal(4, results.ok)
      assert_equal(0, results.locks_set)
      assert_equal(0, results.undefined)
      _checkKeyDoesNotExist(@testTime.to_s + key + i.to_s)
      
      # try again (should be successful with 0 deletes)
      ok = rdht.delete(@testTime.to_s + key + i.to_s)
      assert_equal(0, ok)
      results = rdht.get_last_delete_result()
      assert_equal(0, results.ok)
      assert_equal(0, results.locks_set)
      assert_equal(4, results.undefined)
      _checkKeyDoesNotExist(@testTime.to_s + key + i.to_s)
    end
    
    c.close()
  end
end
