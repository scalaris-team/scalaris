/**
 * Scalaris client for YCSB
 *
 */

package com.yahoo.ycsb.db;

import com.ericsson.otp.erlang.OtpErlangException;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

import de.zib.scalaris.Connection;
import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.ConnectionFactory;
import de.zib.scalaris.NotFoundException;
import de.zib.scalaris.ReplicatedDHT;
import de.zib.scalaris.TransactionSingleOp;

import java.util.Map;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;


public class ScalarisClient extends DB {

	public static final String HOST_PROPERTY = "scalaris.node";
	public static final String CLIENT_NAME_PROPERTY = "scalaris.name";
	public static final String COOKIE_PROPERTY = "scalaris.cookie";

	public static final String TABLE_SEPERATOR = ":";

	private Connection conn;
	private TransactionSingleOp ts;
	private ReplicatedDHT rdht;

	public void init() {
		try {
			Properties props = this.getProperties();
			if (props.isEmpty()) {
				props.setProperty(HOST_PROPERTY, "node1@localhost");
				props.setProperty(CLIENT_NAME_PROPERTY, "YCSB");
				props.setProperty(COOKIE_PROPERTY, "chocolate chip cookie");
			}
			ConnectionFactory fac = new ConnectionFactory(props);
			conn = fac.createConnection();
			ts = new TransactionSingleOp(conn);
			rdht = new ReplicatedDHT(conn);
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
	}

	public void cleanup() throws DBException {
		ts.closeConnection();
	}

	@Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {

		try {
			// get the value
			Map<String, Object> dbValues = ts.read(
					table + TABLE_SEPERATOR + key).jsonValue();
			// jsonValue() returns Map<String, Object> but we need
			// Map<String, ByteIterator>
			for (Entry<String, Object> entry : dbValues.entrySet()) {
				result.put(entry.getKey(), new StringByteIterator(
						(String) entry.getValue()));
			}
		} catch (NotFoundException e) {
		} catch (OtpErlangException e) {
			e.printStackTrace();
			return 1;
		}
		return 0;
	}

	@Override
	public int insert(String table, String key,
			HashMap<String, ByteIterator> values) {

		// Convert <String, ByteIterator> to <String, Object>
		HashMap<String, String> values2 = new HashMap<String, String>(
				values.size());
		for (Entry<String, ByteIterator> entry : values.entrySet()) {
			values2.put(entry.getKey(), entry.getValue().toString());
		}

		try {
			ts.write(table + TABLE_SEPERATOR + key, values2);
		} catch (OtpErlangException e) {
			e.printStackTrace();
			return 1;
		}
		return 0;
	}

	@Override
	public int delete(String table, String key) {

		try {
			// Erlang delete is not save, so we check if the key
			// was deleted on all four replicas
			if(rdht.delete(table + TABLE_SEPERATOR + key).ok != 4)
				return 1;		
		} catch (OtpErlangException e) {
			e.printStackTrace();
			return 1;
		}
		return 0;
	}

	@Override
	/*
	 * We have to read the old values, compare them with the new ones which
	 * leads to replacing or adding of fields
	 */
	public int update(String table, String key,
			HashMap<String, ByteIterator> values) {
		// Convert <String, ByteIterator> to <String, String>
		HashMap<String, String> values2 = new HashMap<String, String>(
				values.size());
		for (Entry<String, ByteIterator> entry : values.entrySet()) {
			values2.put(entry.getKey(), entry.getValue().toString());
		}

		// jsonValue() returns Map<String, Object> but we need
		// Map<String, ByteIterator>
		HashMap<String, String> dbValues2 = new HashMap<String, String>();
		try {
			HashMap<String, Object> dbValues = (HashMap<String, Object>) ts
					.read(table + TABLE_SEPERATOR + key).jsonValue();

			for (Entry<String, Object> entry : dbValues.entrySet()) {
				dbValues2.put(entry.getKey(), (String) entry.getValue());
			}
		} catch (NotFoundException e) {
		} catch (OtpErlangException e) {
			e.printStackTrace();
		}

		// update or add new fields and values
		dbValues2.putAll(values2);

		try {
			ts.write(table + TABLE_SEPERATOR + key, dbValues2);
		} catch (OtpErlangException e) {
			e.printStackTrace();
			return 1;
		}
		return 0;
	}

	@Override
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		/* Scalaris doesn't support scan semantics (sorry) */
		return 1;
	}
}
