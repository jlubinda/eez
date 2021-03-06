import unittest
from jsql import *

class TestJSQL(unittest.TestCase):
	
	def test_selectall_1A(self):
		jsql_qry1 = {"SELECTALL":{
			"FROM":"table1",
			"WHERE":[
					['INIT (',{
						"col1":['=','value1']
					}],
					['AND (',{
						"col2":['LIKE','value2']
					}],
					['OR',{
						"col3":['=','value3']
					}],
					['NOT',{
						"col4":['IN',{"SELECTALL":{"FROM":"table2","WHERE":[
						['INIT',{"col5":['=','value4']}],['END']
						]
						}}]
					}],
					[') END']
				],
			"GROUPBY":["col6","col7","col8"],
			"ORDERBY":[
					["col9","col10","col11"],"DESC"
				]
			}
		}

		sql_qry1 = "SELECT * FROM table1 WHERE (col1 = $1 AND (col2 LIKE $2 OR col3 = $3 NOT col4 IN (SELECT * FROM table2 WHERE col5 = $4))) GROUP BY col6,col7,col8 ORDER BY col9,col10,col11 DESC"
		
		params = ['value1','value2','value3','value4']
		
		js = jsql()
		params_1 = []
		qry_1,params_1,my_key,output = js.jsonDecoder(jsql_qry1)
		
		assert (qry_1 == sql_qry1)
	
	def test_selectall_1B(self):
		jsql_qry1 = {"SELECTALL":{
			"FROM":"table1",
			"WHERE":[
					['INIT (',{
						"col1":['=','value1']
					}],
					['AND (',{
						"col2":['LIKE','value2']
					}],
					['OR',{
						"col3":['=','value3']
					}],
					[') NOT',{
						"col4":['IN',{"SELECTALL":{"FROM":"table2","WHERE":[
						['INIT',{"col5":['=','value4']}],['END']
						]
						}}]
					}],
					[') END']
				],
			"GROUPBY":["col6","col7","col8"],
			"ORDERBY":[
					["col9","col10","col11"],"DESC"
				]
			}
		}

		sql_qry1 = "SELECT * FROM table1 WHERE (col1 = $1 AND (col2 LIKE $2 OR col3 = $3) NOT col4 IN (SELECT * FROM table2 WHERE col5 = $4)) GROUP BY col6,col7,col8 ORDER BY col9,col10,col11 DESC"
		
		qry_params_1 = ['value1','value2','value3','value4']
		
		js = jsql()
		
		params_1 = []
		qry_1,params_1,my_key,output = js.jsonDecoder(jsql_qry1)
		
		assert (qry_params_1 == params_1)

	def test_selectall_1C(self):
		jsql_qry1 = {"SELECTALL":{
			"FROM":"table1",
			"WHERE":[
					['INIT (',{
						"col1":['=','value1']
					}],
					['AND (',{
						"col2":['LIKE','value2']
					}],
					['OR',{
						"col3":['=','value3']
					}],
					['NOT',{
						"col4":['IN',{"SELECTALL":{"FROM":"table2","WHERE":[
						['INIT',{"col5":['=','value4']}],['END']
						]
						}}]
					}],
					['END']
				],
			"GROUPBY":["col6","col7","col8"],
			"ORDERBY":[
					["col9","col10","col11"],"DESC"
				]
			}
		}

		sql_qry1 = "SELECT * FROM table1 WHERE (col1 = $1 AND (col2 LIKE $2 OR col3 = $3 NOT col4 IN (SELECT * FROM table2 WHERE col5 = $4))) GROUP BY col6,col7,col8 ORDER BY col9,col10,col11 DESC"
		
		params = ['value1','value2','value3','value4']
		
		js = jsql()
		
		params_1 = []
		
		qry_1,params_1,my_key,output = js.jsonDecoder(jsql_qry1)
		
		assert (qry_1 == sql_qry1)
	
	def test_selectall_2A(self):
		jsql_qry1 = {"SELECTALL":{
			"FROM":"table1",
			"WHERE":[
					['INIT ',{
						"col1":['=','value1']
					}],
					['AND (',{
						"col2":['LIKE','value2']
					}],
					['OR',{
						"col3":['=','value3']
					}],
					['NOT',{
						"col4":['=','value4']
					}],
					[') END']
				],
			"GROUPBY":["col6","col7","col8"],
			"ORDERBY":[
					["col9","col10","col11"],"DESC"
				]
			}
		}

		sql_qry2 = "SELECT * FROM table1 WHERE col1 = $1 AND (col2 LIKE $2 OR col3 = $3 NOT col4 = $4) GROUP BY col6,col7,col8 ORDER BY col9,col10,col11 DESC"
		
		params_2 = ['value1','value2','value3','value4']
		
		js = jsql()
		params_1 = []
		
		sql_qry1,params_1,my_key,output = js.jsonDecoder(jsql_qry1)
		
		assert sql_qry1 == sql_qry2
	
	def test_selectall_2B(self):
		jsql_qry1 = {"SELECTALL":{
			"FROM":"table1",
			"WHERE":[
					['INIT ',{
						"col1":['=','value1']
					}],
					['AND (',{
						"col2":['LIKE','value2']
					}],
					['OR',{
						"col3":['=','value3']
					}],
					['NOT',{
						"col4":['=','value4']
					}],
					[') END']
				],
			"GROUPBY":["col6","col7","col8"],
			"ORDERBY":[
					["col9","col10","col11"],"DESC"
				]
			}
		}

		sql_qry2 = "SELECT * FROM table1 WHERE col1 = $1 AND (col2 LIKE $2 OR col3 = $3 NOT col4 = $4) GROUP BY col6,col7,col8 ORDER BY col9,col10,col11 DESC"
		
		params_2 = ['value1','value2','value3','value4']
		
		js = jsql()
		params_1 = []
		
		sql_qry1,params_1,my_key,output = js.jsonDecoder(jsql_qry1)
		
		assert params_1 == params_2

	
	##################################
	def test_select_1A(self):
		jsql_qry1 = {"SELECT":{"COLS":['col1','col2','col3'],
			"FROM":"table1",
			"WHERE":[
					['INIT (',{
						"col1":['=','value1']
					}],
					['AND (',{
						"col2":['LIKE','value2']
					}],
					['OR',{
						"col3":['=','value3']
					}],
					['NOT',{
						"col4":['IN',{"SELECTALL":{"FROM":"table2","WHERE":[
						['INIT',{"col5":['=','value4']}],['END']
						]
						}}]
					}],
					[')) END']
				],
			"GROUPBY":["col6","col7","col8"],
			"ORDERBY":[
					["col9","col10","col11"],"DESC"
				]
			}
		}

		sql_qry1 = "SELECT col1,col2,col3 FROM table1 WHERE (col1 = $1 AND (col2 LIKE $2 OR col3 = $3 NOT col4 IN (SELECT * FROM table2 WHERE col5 = $4))) GROUP BY col6,col7,col8 ORDER BY col9,col10,col11 DESC"
		
		
		js = jsql()
		
		params_1 = []
		
		qry_1,params_1,my_key,output = js.jsonDecoder(jsql_qry1)
		
		#print(qry_1)
		
		assert (qry_1 == sql_qry1)
		
	
	
	def test_select_1B(self):
		jsql_qry1 = {"SELECT":{"COLS":['col1','col2','col3'],
			"FROM":"table1",
			"WHERE":[
					['INIT (',{
						"col1":['=','value1']
					}],
					['AND (',{
						"col2":['LIKE','value2']
					}],
					['OR',{
						"col3":['=','value3']
					}],
					['NOT',{
						"col4":['IN',{"SELECTALL":{"FROM":"table2","WHERE":[
						['INIT',{"col5":['=','value4']}],['END']
						]
						}}]
					}],
					[')) END']
				],
			"GROUPBY":["col6","col7","col8"],
			"ORDERBY":[
					["col9","col10","col11"],"DESC"
				]
			}
		}

		sql_qry1 = "SELECT col1,col2,col3 FROM table1 WHERE (col1 = $1 AND (col2 LIKE $2 OR col3 = $3) NOT col4 IN (SELECT * FROM table2 WHERE col5 = $4)) GROUP BY col6,col7,col8 ORDER BY col9,col10,col11 DESC"
		
		qry_params_1 = ['value1','value2','value3','value4']
		
		js = jsql()
		
		params_1 = []
		
		qry_1,params_1,my_key,output = js.jsonDecoder(jsql_qry1)
		
		#print("params_1: "+str(params_1))
		
		assert (qry_params_1 == params_1)
	
	
	def test_select_2A(self):
		jsql_qry1 = {"SELECT":{"COLS":['col1','col2','col3'],
			"FROM":"table1",
			"WHERE":[
					['INIT ',{
						"col1":['=','value1']
					}],
					['AND (',{
						"col2":['LIKE','value2']
					}],
					['OR',{
						"col3":['=','value3']
					}],
					['NOT',{
						"col4":['=','value4']
					}],
					[') END']
				],
			"GROUPBY":["col6","col7","col8"],
			"ORDERBY":[
					["col9","col10","col11"],"DESC"
				]
			}
		}

		sql_qry2 = "SELECT col1,col2,col3 FROM table1 WHERE col1 = $1 AND (col2 LIKE $2 OR col3 = $3 NOT col4 = $4) GROUP BY col6,col7,col8 ORDER BY col9,col10,col11 DESC"
		
		params_2 = ['value1','value2','value3','value4']
		
		js = jsql()
		
		params_1 = []
		
		sql_qry1,params_1,my_key,output = js.jsonDecoder(jsql_qry1)
		
		assert sql_qry1 == sql_qry2


	
	def test_select_2B(self):
		jsql_qry1 = {"SELECT":{"COLS":['col1','col2','col3'],
			"FROM":"table1",
			"WHERE":[
					['INIT ',{
						"col1":['=','value1']
					}],
					['AND (',{
						"col2":['LIKE','value2']
					}],
					['OR',{
						"col3":['=','value3']
					}],
					['NOT',{
						"col4":['=','value4']
					}],
					[') END']
				],
			"GROUPBY":["col6","col7","col8"],
			"ORDERBY":[
					["col9","col10","col11"],"DESC"
				]
			}
		}

		sql_qry2 = "SELECT col1,col2,col3 FROM table1 WHERE col1 = $1 AND (col2 LIKE $2 OR col3 = $3 NOT col4 = $4) GROUP BY col6,col7,col8 ORDER BY col9,col10,col11 DESC"
		
		params_2 = ['value1','value2','value3','value4']
		
		js = jsql()
		
		params_1 = []
		
		sql_qry1,params_1,my_key,output = js.jsonDecoder(jsql_qry1)
		
		assert params_1 == params_2

	
	
	def test_delete_1A(self):
		jsql_qry1 = {"DELETE":{"FROM":"table1",
			"WHERE":[
					['INIT ',{
						"col1":['=','value1']
					}],
					['AND (',{
						"col2":['LIKE','value2']
					}],
					['OR',{
						"col3":['=','value3']
					}],
					['NOT',{
						"col4":['=','value4']
					}],
					[') END']
				]
			}
		}

		sql_qry2 = "DELETE FROM table1 WHERE col1 = $1 AND (col2 LIKE $2 OR col3 = $3 NOT col4 = $4)"
		
		params_2 = ['value1','value2','value3','value4']
		
		js = jsql()
		
		params_1 = []
		
		sql_qry1,params_1,my_key,output = js.jsonDecoder(jsql_qry1)
		
		assert sql_qry1 == sql_qry2


	
	def test_delete_1B(self):
		jsql_qry1 = {"DELETE":{"FROM":"table1",
			"WHERE":[
					['INIT ',{
						"col1":['=','value1']
					}],
					['AND (',{
						"col2":['LIKE','value2']
					}],
					['OR',{
						"col3":['=','value3']
					}],
					['NOT',{
						"col4":['=','value4']
					}],
					[') END']
				]
			}
		}

		sql_qry2 = "DELETE FROM table1 WHERE col1 = $1 AND (col2 LIKE $2 OR col3 = $3 NOT col4 = $4)"
		
		params_2 = ['value1','value2','value3','value4']
		
		js = jsql()
		
		params_1 = []
		
		sql_qry1,params_1,my_key,output = js.jsonDecoder(jsql_qry1)
		
		assert params_1 == params_2
	
	
	def test_insert_1A(self):
		jsql_qry1 = {"INSERT":{"INTO":"table1",
			"COLS":['col1','col2','col3'],
			"VALUES":['value1','value2','value3']
			}
		}

		sql_qry2 = "INSERT INTO table1 (col1,col2,col3) VALUES ($1,$2,$3)"
		
		params_2 = ['value1','value2','value3']
		
		js = jsql()
		
		params_1 = []
		
		sql_qry1,params_1,my_key,output = js.jsonDecoder(jsql_qry1)
		
		assert sql_qry1 == sql_qry2


	
	def test_insert_1B(self):
		jsql_qry1 = {"INSERT":{"INTO":"table1",
			"COLS":['col1','col2','col3'],
			"VALUES":['value1','value2','value3']
			}
		}

		sql_qry2 = "INSERT INTO table1 (col1,col2,col3) VALUES ($1,$2,$3)"
		
		params_2 = ['value1','value2','value3']
		
		js = jsql()
		
		params_1 = []
		
		sql_qry1,params_1,my_key,output = js.jsonDecoder(jsql_qry1)
		
		assert params_1 == params_2
	
	
	def test_update_1A(self):
		jsql_qry1 = {"UPDATE":{"TABLE":"table1",
			"SET":{"col1":"value1","col2":"value2","col3":"value3"},
			"WHERE":[
					['INIT ',{
						"col4":['=','value4']
					}],
					['AND (',{
						"col5":['LIKE','value5']
					}],
					['OR',{
						"col6":['=','value6']
					}],
					['NOT',{
						"col7":['=','value7']
					}],
					[') END']
				]
			}
		}

		sql_qry2 = "UPDATE table1 SET col1=$1,col2=$2,col3=$3 WHERE col4 = $4 AND (col5 LIKE $5 OR col6 = $6 NOT col7 = $7)"
		
		params_2 = ['value1','value2','value3','value4','value5','value6','value7']
		
		js = jsql()
		
		params_1 = []
		
		sql_qry1,params_1,my_key,output = js.jsonDecoder(jsql_qry1)
		
		assert sql_qry1 == sql_qry2


	
	def test_update_1B(self):
		jsql_qry1 = {"UPDATE":{"TABLE":"table1",
			"SET":{"col1":"value1","col2":"value2","col3":"value3"},
			"WHERE":[
					['INIT ',{
						"col4":['=','value4']
					}],
					['AND (',{
						"col5":['LIKE','value5']
					}],
					['OR',{
						"col6":['=','value6']
					}],
					['NOT',{
						"col7":['=','value7']
					}],
					[') END']
				]
			}
		}

		sql_qry2 = "UPDATE table1 SET col1=$1,col2=$2,col3=$3 WHERE col4 = $4 AND (col5 LIKE $5 OR col6 = $6 NOT col7 = $7)"
		
		params_2 = ['value1','value2','value3','value4','value5','value6','value7']
		
		js = jsql()
		
		params_1 = []
		
		sql_qry1,params_1,my_key,output = js.jsonDecoder(jsql_qry1)
		
		assert params_1 == params_2

		
	def test_process_1A(self):

		jsql_qry = {"PROCESS":{
			"DATA":[12,5,1.97],
			"USING":"test1",
			"FROM":"jsql.tests"
			}
		}
		js = jsql()
		qry,params,my_key,output = js.jsonDecoder(jsql_qry)
		
		assert(int(output)==6)

	
	
	def test_process_1B(self):

		jsql_qry = {"PROCESS":{
			"DATA":[[1,3,5,7,9,11,13,15,17,19,21],[2,4,6,8,10,12,14,16,18,20,22]],
			"USING":"test2",
			"FROM":"jsql.tests",
			"APPEND":{"PER_RESULT":{"PROCESS":{
						"DATA_REFERENCE":{"RES_LIST_OUTPUT":"all"},
						"DATA":[21,42,63,84,105,126,147,168,189,210,222],
						"USING":"test3",
						"FROM":"jsql.tests"
						}
					}
				}
			}
		}
		
		js = jsql()
		sql_qry1,params,my_key,output = js.jsonDecoder(jsql_qry)
		# test_output = [3.0, 138.0, 885.0, 3108.0, 8055.0, 17358.0, 33033.0, 57480.0, 93483.0, 144210.0, 213213.0]
		
		#Jason# - check this output list to see if it is the intended behaviour
		test_output = [31.5, 483.0, 1858.5, 4662.0, 9397.5, 16569.0, 26680.5, 40236.0, 57739.5, 79695.0, 102453.0]
		
		# print(output)
		
		self.assertEqual(test_output, output)
		# assert (test_output == output)
		

	#"DATA_REFERENCE":{"RES_LIST_VALUE_BY_KEY":["result_key1","result_key2","result_key3","result_key4"]}
	#"DATA_REFERENCE":{"RES_LIST_OUTPUT":"all"}
	#"DATA_REFERENCE":{"RES_LIST_OUTPUT":3:9}

	
	def test_truncate(self):

		jsql_qry = {"TRUNCATE":"test1"}
		
		js = jsql()
		sql_qry1,params,my_key,output = js.jsonDecoder(jsql_qry)
		test_output = "TRUNCATE test1"
		assert(test_output==sql_qry1)

	def test_process_1C(self):

		jsql_qry = {"PROCESS":{
			"DATA":['data_item_1/argument_1','data_item_2/argument_2','data_item_3/argument_3','data_item_4/argument_4','data_item_5/argument_5'],
			"USING":"method",
			"FROM":"module.submodule.myclass",
			"APPEND":{"TO_RESULT":{"PROCESS":{
						"DATA":['data_item_1/argument_1','data_item_2/argument_2','data_item_3/argument_3','data_item_4/argument_4','data_item_5/argument_5'],
						"USING":"method",
						"FROM":"jsql.tests.test3"
						}
					}
				}
			}
		}