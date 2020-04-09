import mysql.connector
import re
import logging
import validators
  
# from Djando 1.6
# ATT: it only gets well formed urls
regex = re.compile(
    r'^(?:http|ftp)s?://'  # http:// or https://
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
    r'localhost|'  # localhost...
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|'  # ...or ipv4
    r'\[?[A-F0-9]*:[A-F0-9:]+\]?)'  # ...or ipv6
    r'(?::\d+)?'  # optional port
    r'(?:/?|[/?]\S+)$', re.IGNORECASE)

# get urls without the protocol
regex2 = re.compile(r'www.?(?:[-\w.]|(?:%[\da-fA-F]{2}))+', re.IGNORECASE)

	

logger = logging.getLogger("root")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)



sql_select = "SELECT * FROM entidade"
sql_update = "UPDATE entidade SET WEBSITE = %s WHERE ID_ENTIDADE = %s"

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="",
  database="XXX"
)


mycursor = mydb.cursor(dictionary=True)#, buffered=True)

mycursor.execute(sql_select)

urls = []


counter = 0		# just to make sure something is happening
for record in mycursor:
	counter += 1
	if counter % 5000 == 0:
		logger.info ("## {} ##".format(counter))
	
	if record['WEBSITE'] is None:
		url = None
		if record['NOME']:
			url = re.findall(regex, record['NOME'])
			if not url or len(url) == 0:
				url = re.findall(regex2, record['NOME'])
		if not url or len(url) == 0:
			if record['MORADA']:
				url = re.findall(regex, record['MORADA'])
				if not url or len(url) == 0:
					url = re.findall(regex2, record['MORADA'])				
		if url and len(url) > 0:
			isGood = validators.url("http://" + url[0]) if "http://" not in url[0].lower() else validators.url(url[0])
			if isGood:
				logger.info ("website: {} # entidade: {}".format(url[0], record['ID_ENTIDADE']))
				val = (url[0], record['ID_ENTIDADE'])
				urls.append(val)
				logger.info ("So far: {}".format(len(urls)))
					


logger.info ("------------------------------------------")
logger.info ("Total urls: {}".format(len(urls)))
logger.info ("Saving ...")

for rec in urls:
	mycursor.execute(sql_update, rec)			
	mydb.commit()


