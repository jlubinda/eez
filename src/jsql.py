import psycopg2
import asyncpg
import asyncio
import re
from pydoc import locate

class jsql:
	def __init__(self):
		pass

	def sanitize(self, my_input, user_input=False): 
		if user_input == False:
			my_input = re.sub('[^A-Za-z0-9_,.]+', '', my_input)		  
		else:
			pass 
		return my_input
	
	def operation(self, operator, x, y):
		return {'add': lambda: x+y, 'sub': lambda: x-y, 'mul': lambda: x*y,'div':lambda: x/y,}.get(operator, lambda: "Not a valid operation")()
	
	def getValue(self,mydict,key,alt=False):
		return mydict.get(key,lambda: alt)()
	
	def checkString(self,my_string,my_list):
		a=0
		for item in my_list:
			myString = item.lower()
			if myString in my_string:
				a = a+1
		return a
	
	def decodeInsert(self,mydict):
		return "",""
	
	def decodeSelectAll(self,mydict):
		return "",""
	
	def decodeSelectAll2(self,mydict):
		return "",""
	
	def decodeSelect2(self,mydict):
		return "",""
		
	def fromDecode(self,my_input):
		if type(my_input) is dict:
		
			my_key = list(my_input.keys())[0]
			my_dict = my_input[my_key]
			
			if my_key.upper()=="SELECTALL":
				qry,params = self.decodeSelectAll2(my_dict)
			elif my_key.upper()=="SELECT":
				qry,params = self.decodeSelect2(my_dict)
			
			output = "FROM ("+qry+") "
		else:
			output = "FROM "+self.sanitize(str(my_input))+" "
			params = None
		
		return output,params
		
	def openingBracketProcessor(self,my_string):
		if my_string[len(my_string)-1]=="(":
			return "("
		else:
			return ""
		
	def closingBracketProcessor(self,my_string):
		if my_string[0]==")":
			return ")"
		else:
			return ""
		
		
	def whereDecode(self,my_where_input):
		output = ""
		params_output = []
		for item in my_where_input:
			jsql_operator = item[0]
			my_item_key = list(item[1].keys())[0]
			
			my_item_operator = item[1][my_item_key][0]
			my_item_value = item[1][my_item_key][1]
			
			
			if (type(my_item_value) is dict) or (type(my_item_value) is list) or (type(my_item_value) is tuple):
			
				my_key = list(my_item_value.keys())[0]
				my_dict = my_item_value[my_key]
				
				if my_key.upper()=="SELECTALL":
					qry,params = self.decodeSelectAll2(my_dict)
				elif my_key.upper()=="SELECT":
					qry,params = self.decodeSelect2(my_dict)
				
				output = output+self.openingBracketProcessor(jsql_operator)+self.closingBracketProcessor(jsql_operator)
				output = output+self.sanitize(str(my_item_key))+" "
				output = output+self.sanitize(str(my_item_operator))+" "
				output = output+"("+qry+") "
				output = output+self.closingBracketProcessor(jsql_operator)+self.openingBracketProcessor(jsql_operator)
				
				params_output = params_output+params
			else:
				output = output+self.openingBracketProcessor(jsql_operator)+self.closingBracketProcessor(jsql_operator)
				output = output+self.sanitize(str(my_item_key))+" "
				output = output+self.sanitize(str(my_item_operator))+" "
				output = output+"'"+self.sanitize(str(my_item_value))+"' "
				output = output+self.closingBracketProcessor(jsql_operator)+self.openingBracketProcessor(jsql_operator)
		
		return output,params_output
	
	def decodeSelect(self,mydict):
		cols = mydict["COLS"]
		
		fromTxt,fromParams = self.fromDecode(self,mydict["FROM"])
		whereTxt,whereParams = self.whereDecode(self,mydict["FROM"])
		
		qry = "SELECT "+self.sanitize(str(cols))+" "+fromTxt+""+whereTxt
		return qry,""
	
	def decodeUpdate(self,mydict):
		return "",""
	
	def decodeDelete(self,mydict):
		return "",""
	
	def decodeTruncate(self,mydict):
		return ""
	
	def decodeProcess(self,mydict):
		return "",""
		
	def checkDictWithKey(self,mydict,key):
		if mydict.get(mydict[key],lambda: None)()== None:
			return None
		else:
			return key
		
	def getDictWithKey(self,mydict,key):
		return mydict.get(mydict[key],lambda: None)()
		
	def runProcess(self,module_submodule_and_class,mymethod,params):
		my_class = locate(module_submodule_and_class) # eg "module.submodule.class"
		instance = my_class()
		return getattr(instance, mymethod)(*params) # my method is the function being called to process the data. params is a list containing the parameters which the method takes in.
		
	def processor(self,my_dict):
		module_submodule_and_class = my_dict["FROM"]
		mymethod = my_dict["USING"]
		params = my_dict["DATA"]
		appendData = my_dict["APPEND"]
		res = self.runProcess(module_submodule_and_class,mymethod,params)
		
		return res,appendData
		
	def processKeyWords(self,mydict):
		my_key = list(mydict.keys())[0]
		my_dict = mydict[my_key]
		
		if my_key.upper()=="SELECTALL":
			qry,params = self.decodeSelectAll(my_dict)
		elif my_key.upper()=="SELECT":
			qry,params = self.decodeSelect(my_dict)
		elif my_key.upper()=="INSERT":
			qry,params = self.decodeInsert(my_dict)
		elif my_key.upper()=="UPDATE":
			qry,params = self.decodeUpdate(my_dict)
		elif my_key.upper()=="DELETE":
			qry,params = self.decodeDelete(my_dict)
		elif my_key.upper()=="TRUNCATE":
			qry = self.decodeTruncate(my_dict)
			params = None
		elif my_key.upper()=="PROCESS":
			qry = my_dict
			params = my_dict["DATA"]
		
		return qry,params,my_dict,my_key.upper()
		
	def jsonDecoder(self,mydict):
	
		qry,params,my_dict,my_key = self.processKeyWords(mydict)
		if my_key.upper()=="PROCESS":
			res,appendData1 = self.processor(my_dict)
			try:
				to_result = appendData1["TO_RESULT"]
				resx,paramsx,my_dictx = self.processKeyWords(to_result)
				res = qry
				output = []
				#output["res_items"] = []
				#output["appended_items"] = []
				a = -1
				for item in res:
					a = a+1
					qry2,params2,my_dict2,my_key2 = self.processKeyWords(item)
					if my_key2.upper()=="PROCESS":
						res2,appendData2 = self.processor(my_dict2)
						output.append({"res_items":item,"appended_items":res2})
					else:
						output.append({"res_items":item,"appended_items":qry2})
			except:
				try:
					per_result = appendData1["PER_RESULT"]
					resx,params,my_dict = self.processKeyWords(per_result)
					res = qry
					output = []
					#output["res_items"] = []
					#output["appended_items"] = []
					a = -1
					for item in res:
						a = a+1
						qry2,params2,my_dict2,my_key2 = self.processKeyWords(item)
						if my_key2.upper()=="PROCESS":
							res2,appendData2 = self.processor(my_dict2)
							output.append({"res_items":item,"appended_items":res2})
						else:
							output.append({"res_items":item,"appended_items":qry2})
				except:
					output = None
		else:
			output = None
		return qry,params,my_key,output
		
		
	async def runQuery(self,con,mydict):
		qerrors = []
		
		
		num_queries = len(mydict)
		
		if num_queries>1:
			qry = ""
			c = 0
			qry_list = []
			keys_list = []
			params_list = []
			output = []
			for dict_item in mydict:
				c = c+1
				
				qry,params,my_key,output = self.jsonDecoder(dict_item)
				
				qry_list.append(qry)
				keys_list.append(my_key)
				params_list.append(params)
				
				if params==None:
					await output.append(con.execute(qry))
				else:
					try:
						myparam = params[0]
						await output.append(con.execute(qry,*params))
					except:
						await output.append(con.execute(qry,params))
		else:
			qry,params,my_key,output = self.jsonDecoder(mydict)
			
			if params==None:
				output = await con.execute(qry)
			else:
				try:
					myparam = params[0]
					output = await con.execute(qry,*params)
				except:
					output = await con.execute(qry,params)
		
		return qry,params,my_key,output
		#row = await conn.fetchrow( 'SELECT * FROM users WHERE name = $1', 'Bob')
		#await conn.execute('INSERT INTO users(name, dob) VALUES($1, $2)', 'Bob', datetime.date(1984, 3, 1))
		
		"""
		import json

        conn = await self.cluster.connect(database='postgres', loop=self.loop)
        try:
            def _encoder(value):
                return b'\x01' + json.dumps(value).encode('utf-8')

            def _decoder(value):
                return json.loads(value[1:].decode('utf-8'))

            await conn.set_type_codec(
                'jsonb', encoder=_encoder, decoder=_decoder,
                schema='pg_catalog', binary=True
            )

            data = {'foo': 'bar', 'spam': 1}
            res = await conn.fetchval('SELECT $1::jsonb', data)
            self.assertEqual(data, res)

        finally:
            await conn.close()
		
		"""
		
		#await conn.set_type_codec('jsonb', encoder=_encoder, decoder=_decoder, schema='pg_catalog', format='binary')
		
		"""
		#mydict = [{"select":[col1, col2, col3, col4],"from":"table1", "where":{"col1":[">",5],"and":{"col2":["<",9],"or":[]}}},{table2:[col1, col2, col3, col4]}]
		#mydict = [{"select":[col1, col2, col3, col4],"from":"table1", "innerjoin":"table2","on":"colx"},{table2:[col1, col2, col3, col4]}]
		#SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate FROM Orders INNER JOIN Customers ON Orders.CustomerID=Customers.CustomerID;
		#SELECT column_name(s) FROM table1 LEFT JOIN table2 ON table1.column_name = table2.column_name;
		#SELECT column_name(s) FROM table1 RIGHT JOIN table2 ON table1.column_name = table2.column_name;
		#SELECT column_name(s) FROM table1 FULL OUTER JOIN table2 ON table1.column_name = table2.column_name WHERE condition;
		#SELECT A.CustomerName AS CustomerName1, B.CustomerName AS CustomerName2, A.City FROM Customers A, Customers B WHERE A.CustomerID <> B.CustomerID AND A.City = B.City ORDER BY A.City;
			
			
			SELECTALL:
				FROM:if text, value in a table
				if dictionary, value is a query
				WHERE:
					[
						['INIT',{
							"col1":['=',value1]
						}],
						['AND',{
							"col2":['LIKE',value2]
						}],
						['OR',{
							"col3":['=',value3]
						}],
						['NOT',{
							"col4":['IN',{qry1}]
						}]
					]
				GROUPBY:
					[col1,col2,col3]
				ORDERBY:
					[
						[col1,col2,col3],
						"DESC/ASC"
					]
				
			SELECTALL
				INTO: 
				target table name
			
			SELECT
				DISTINCT: 
				text with column name 
				list with column names
				
			SELECT
				MIN
				if text, value in a table
				if dictionary, value is a query
				
			SELECTMAX
				if text, value in a table
				if dictionary, value is a query
				
			SELECT
				COUNT
				if text, value in a table
				if dictionary, value is a query
				
			SELECT
				AVG
				if text, value in a table
				if dictionary, value is a query
				
			SELECT
				SUM
				if text, value in a table
				if dictionary, value is a query
				
			UPDATE: table name
				SET: dictionary of columns and values
					{"col1":value1,"col2":value2}
				WHERE: 
					{}
			DELETE
			INSERT
				INTO
			TRUNCATE
			CASE:list containing dictionary of queries
				[{qry1},{qry2},{qry3}] END
				[{
					"WHEN": text if single condition or dictionary if condition on query,
					"THEN": text if single condition or dictionary if condition on query
				},
				{
					"WHEN": text if single condition or dictionary if condition on query,
					"THEN": text if single condition or dictionary if condition on query
				},
				{
					"WHEN": text if single condition or dictionary if condition on query,
					"THEN": text if single condition or dictionary if condition on query
				}] END
			
			
			
			#MYSQL# IFNULL[COL,ReplacementVal]
			#MYSQL# COALESCE[COL,ReplacementVal]
		"""
		
		"""
		qry = ""
		errors = []
		ops = ["SELECTALL","SELECT","UPDATE","DELETE","INSERT","TRUNCATE","CASE"]
		for row in mydict:
			#keys = list(mydict.keys())
			x = -1
			for key in mydict.keys():
				x = x+1
				op = key.upper()
				if x==0:
					if op in ops:
						if checkPerms(role,op)==True:
							if op=="SELECTDISTINCT":
								qry = qry+str("SELECT DISTINCT")+" "+self.sanitize(str(mydict[key]))
							else:
								qry = qry+str(op)+" "+self.sanitize(str(mydict[key]))								
						else:
							errors.append(["8100","Access denied"])
							break;
					else:
						errors.append(["9100","Operation not recognised"])
						break;
				else:
					if key.upper()=="FROM":
						qry = qry+str(op)+" "+self.sanitize(str(mydict[key]))
					elif(key.upper()=="INNERJOIN"):
						
					elif(key.upper()=="OUTJOIN"):
						
					elif(key.upper()=="LEFTJOIN"):
						
					elif(key.upper()=="RIGHTJOIN"):
						
					elif(key.upper()=="FULLOUTERJOIN"):
						
					elif(key.upper()=="UNION"):
						
					elif(key.upper()=="SET"):
						
					elif(key.upper()=="AND"):
						
					elif(key.upper()=="OR"):
						
					elif(key.upper()=="NOT"):
						
					elif(key.upper()=="WHERE"):
						
					elif(key.upper()=="WHEREEXISTS"):
						
					elif(key.upper()=="ANY"):
						
					elif(key.upper()=="ORDERBY"):
						
					elif(key.upper()=="ASC"):
						
					elif(key.upper()=="DESC"):
						
					elif(key.upper()=="ORDERBY"):
						
					elif(key.upper()=="ISNULL"):
						
					elif(key.upper()=="ISNOTNULL"):
						
					elif(key.upper()=="LIKE"):
						
					elif(key.upper()=="IN"):
						
					elif(key.upper()=="BETWEEN"):
						
					elif(key.upper()=="AS"):
						
					elif(key.upper()=="ON"):
						
					elif(key.upper()=="GROUPBY"):
						
					elif(key.upper()=="ORDERBY"):
						
					elif(key.upper()=="HAVING"):
						
					elif(key.upper()=="ORDERBY"):
					"""
	def con(self, conf):
		connection = psycopg2.connect(database = conf["db"], user = conf["user"], password = conf["password"], host = conf["host"], port = conf["port"])
		return connection

	async def aCon(self, conf):
		connection = await asyncpg.connect(host=conf["host"], port=conf["port"], user=conf["user"], password=conf["password"], database=conf["db"])
		return connection

	def cur(self, con):
		mycursor = con.cursor()
		return mycursor

	def searchByJSONBCol(self,cur,tabl,my_col,my_key,my_value,specific_columns):
		qry = "SELECT " +self.sanitize(str(specific_columns))+ "FROM " +self.sanitize(tabl)+ " WHERE ("+self.sanitize(my_col)+"->'"+self.sanitize(my_key)+"') = '"+sanitize(my_value)+"';" 
		cur.execute(qry)
		return cur.fetchall()

	async def aSearchByJSONBCol(self,conf,tabl,my_col,my_key,my_value,specific_columns):
		con = await self.aCon(self, conf)
		qry = "SELECT " +self.sanitize(str(specific_columns))+ "FROM " +self.sanitize(tabl)+ " WHERE ("+self.sanitize(my_col)+"->'"+self.sanitize(my_key)+"') = '"+sanitize(my_value)+"';" 
		output = await con.fetch(qry)
		return output

	def select(self,cur,tabl,my_col,my_key,my_value,specific_columns):
		qry = "SELECT " +self.sanitize(str(specific_columns))+ "FROM " +self.sanitize(tabl)+ " WHERE ("+self.sanitize(my_col)+"->'"+self.sanitize(my_key)+"') = '"+sanitize(my_value)+"';" 
		cur.execute(qry)
		return cur.fetchall()

	async def aSelect(self,conf,tabl,my_col,my_key,my_value,specific_columns):
		con = await self.aCon(self, conf)
		qry = "SELECT " +self.sanitize(str(specific_columns))+ "FROM " +self.sanitize(tabl)+ " WHERE ("+self.sanitize(my_col)+"->'"+self.sanitize(my_key)+"') = '"+sanitize(my_value)+"';" 
		output = await con.fetch(qry)
		return output


class tests:
	def __init__(self):
		pass
	
	def test1(self,param1,param2,param3):
		return (param*(param2/param3))/param2
	
	def test2(self,param1,param2):

		outlist = []
		for i in range(len(param1)):
			out1 = param1[i]
			out2 = param2[i]
			outlist.append(out1*out2)

		return outlist
	
	def test3(self,param1,param2,param3):
		
		outlist = []
		for i in range(len(param1)):
			out1 = param1[i]
			out2 = param2[i]
			outlist.append((out1-0.5)*out2)
		
		return outlist
	
	def test4(self,param1,param2,param3):
		return (param+(param2*param3))*param2

