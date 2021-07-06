import psycopg2
import asyncpg
import asyncio
import re

class JSQL:
	def __init__(self):
		pass
	
	def operation(self, operator, x, y):
		return {'add': lambda: x+y, 'sub': lambda: x-y, 'mul': lambda: x*y,'div':lambda: x/y,}.get(operator, lambda: "Not a valid operation")()
	
	def getValue(self,mydict,key,alt=False):
		return mydict.get(key,lambda: alt)()
	
	def decodeInsert(self,mydict,key,alt=False):
		return mydict.get(key,lambda: alt)()
	
	def decodeSelect(self,mydict,key,alt=False):
		return mydict.get(key,lambda: alt)()
	
	def decodeUpdate(self,mydict,key,alt=False):
		return mydict.get(key,lambda: alt)()
	
	def decodeDelete(self,mydict,key,alt=False):
		return mydict.get(key,lambda: alt)()
	
	def decodeTruncate(self,mydict,key,alt=False):
		return mydict.get(key,lambda: alt)()
		
	def checkDictWithKey(self,mydict,key):
		if mydict.get([mydict[key],lambda: None)()== None:
			return None
		else:
			return key
		
	def getDictWithKey(self,mydict,key):
		return mydict.get([mydict[key],lambda: None)()
		
	
	def decodeJSON(self,mydict):
		qerrors = []
		qry = ""
		
		selectAllQry,myKey = self.checkKey(mydict,"SELECTALL")
		selectQry = self.checkKey(mydict,"SELECT")
		insertQry = self.checkKey(mydict,"INSERT")
		updateQry = self.checkKey(mydict,"UPDATE")
		deleteQry = self.checkKey(mydict,"DELETE")
		truncateQry = self.checkKey(mydict,"TRUNCATE")
		
		qryList1 = list(set([self.getDictWithKey(mydict,"SELECTALL"),self.getDictWithKey(mydict,"SELECT"),self.getDictWithKey(mydict,"INSERT"),self.getDictWithKey(mydict,"UPDATE"),self.getDictWithKey(mydict,"DELETE"),self.getDictWithKey(mydict,"TRUNCATE")]))
		
		qryList = [i for i in qryList1 if i]
		
		try:
			selectall_dict = 
			
			
		except:
			try:
				selecta_dict = 
				
			except:
				try:
					insert_dict = 
					decodeInsert(self,insert_dict,alt=False)
				except:
					try:
						update_dict = 
					except:
						try:
							update_dict = 
						except:
							try:
								translate_dict = 
							except:
								qerrors.append(['9911','Unknown command',0])
		
		qryx = 'INSERT INTO mytable (a) VALUES ($1, $2, $3);'
		
		await con.executemany(qryx,[list of tuples])
		
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
					
	def con(self, conf):
		connection = psycopg2.connect(database = conf["db"], user = conf["user"], password = conf["password"], host = conf["host"], port = conf["port"])
		return connection

	async def aCon(self, conf):
		connection = await asyncpg.connect(host=conf["host"], port=conf["port"], user=conf["user"], password=conf["password"], database=conf["db"])
		return connection

	def cur(self, con):
		mycursor = con.cursor()
		return mycursor

	def sanitize(self, my_input, user_input=False): 
		if user_input == False:
			my_input = re.sub('[^A-Za-z0-9_,.]+', '', my_input)		  
		else:
			pass 
		return my_input

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


