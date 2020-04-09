import mysql.connector
import re
import logging



logger = logging.getLogger("root")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)



sql_select = "SELECT * FROM entidade"
sql_update = "UPDATE entidade SET EMAIL = %s WHERE ID_ENTIDADE = %s"

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="",
  database="XXX"
)


mycursor = mydb.cursor(dictionary=True)#, buffered=True)

mycursor.execute(sql_select)

emails = []

counter = 0		# just to make sure something is happening
for record in mycursor:
	counter += 1
	if counter % 5000 == 0:
		logger.info ("## {} ##".format(counter))
	
	if record['EMAIL'] is None:
		email = None
		if record['NOME']:
			email = re.findall('\S+@\S+',record['NOME'])
		if not email or len(email) == 0:
			if record['MORADA']:
				email = re.findall('\S+@\S+',record['MORADA'])
		if email and len(email) > 0:
			logger.info ("email: {} # entidade: {}".format(email[0], record['ID_ENTIDADE']))
			val = (email[0], record['ID_ENTIDADE'])
			emails.append(val)
			logger.info ("So far: {}".format(len(emails)))			


logger.info ("------------------------------------------")
logger.info ("Total emails: {}".format(len(emails)))
logger.info ("Saving ...")

for rec in emails:
	mycursor.execute(sql_update, rec)			
	mydb.commit()


