"""This module provides static methods to create ascii, csv, and html attachment and send email to specified group of people. """

import time
import sys
import datetime
from email.MIMEText import MIMEText
from email.MIMEImage import MIMEImage
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.Utils import formataddr
from email.quopriMIME import encode
import smtplib



import NiceNum
##########################################
# This code is partially taken from      #
# AccountingReports.py                   #
##########################################
class TextUtils:
	"""Formats the text to create ascii, csv, and html attachment  and send email to specified group of people. """

	def __init__(self,table_header):
		"""Args:
			table_header(list of str) - the header row for the output table
		"""		
		self.table_header=table_header
		
	def getWidth(self,l):
		"""Returns max length of string in the list - needed for text formating of the table
			l(list of str)
		"""

		return max(len(repr(s)) for s in l) 

	def getLength(self,text):
		"""Returns number of rows in the table
		Args:
			text(list)
		"""

		return len(text[self.table_header[0]])

	def printAsTextTable(self,format_type,text):
		""""Prepares input text to send as attachment
		Args:
			format_type(str) - text, csv, html
			text (dict of lists) - {column_name:[values],column_name:[values]} where column_name corresponds to header name
		"""

		#the order is defined by header list
		col_paddings = []
        	message=""
        	if format_type=="text":
                	col=rcol=lcol=ecol=tbcol=tecol=bcol=tcol="|"
                	row="+"
                	space=""
                	for name in self.table_header:
                    		pad=self.getWidth(text[name]+[name,])
                    		col_paddings.append(pad)
                    		for i in range(pad):
                        		row="%s-" % (row)
                    		row="%s-+" %(row)
                	ecol="%s\n%s" % (ecol,row)
                	tecol="%s\n%s" % (tecol,row)
                	message="%s\n" % (row,)
        	else:
                	for name in self.table_header:
                        	col_paddings.append(0)
        	if format_type=="csv":
                	col=","
                	bcol=ecol=tecol=tbcol=""
                	tcol=rcol=lcol=","
                	row=""
                	space=""
        	if format_type=="html":
                	col="</td><td align=center>"
                	tbcol="<tr><th align=center>"
                	tecol="</th></tr>"
			tcol="</th><th align=center>"
                	rcol="</td><td align=right>"
                	lcol="</td><td align=left>"
               	 	bcol="<tr><td align=left>"
                	ecol="</td></tr>"
                	space="&nbsp;"
        	line=""
        	for i in range(len(self.table_header)):
                	pad=col_paddings[i]
                	column = self.table_header[i].center(pad + 1)
                	if i==0:
                        	line=column
                	else:
                        	line="%s%s%s" % (line,tcol,column)
        	message="%s%s%s%s\n" % (message,tbcol,line,tecol)
		for count in range(0,self.getLength(text)):
			index=0	
			line=bcol
			for key in self.table_header:
				item=text[key][count]
				separator=lcol
				if format_type!="csv" and  (type(item)==type(0) or type(item)==type(0.0)):
					separator=rcol
					nv=NiceNum.niceNum(item,1)
					value=nv.rjust(col_paddings[index] + 1)
				else:
					if type(item)==type(0) or type(item)==type(0.0):
						value=repr(item).rjust(col_paddings[index]+1)
					else:
						value=item.ljust(col_paddings[index] + 1)
						if format_type=="html" and  len(item.strip())==0:
							value=space
				if line==bcol:
					line="%s%s" % (line,value)
				else:
					line="%s%s%s" % (line,separator,value)
				index=index+1
			line="%s%s" % (line,ecol)
			message="%s%s\n" % (message,line)
        	return message

def sendEmail( toList, subject, content,fromEmail = None, smtpServerHost=None):
    """
    This turns the "report" into an email attachment and sends it to the EmailTarget(s).
    Args:
	toList(list of str) - list of emails addresses
	content(str) - email content
	fromEmail (str) - from email address
	smtpServerHost(str) - smtpHost 
    """

    if (toList[1] == None):
       print >> sys.stderr ,"Cannot send mail (no To: specified)!"
       sys.exit(1) 

    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = formataddr(fromEmail)
    msg["To"] = _toStr(toList)
    msg1 = MIMEMultipart("alternative")
    # new code
    msgText1=msgText2=None
    if content.has_key("text"):
    	msgText1 = MIMEText("<pre>" + content["text"] + "</pre>", "html")
    	msgText2 = MIMEText(content["text"])
    msgHtml = MIMEText(content["html"], "html")
    msg1.attach(msgHtml)
    if content.has_key("text"):
    	msg1.attach(msgText2)
    	msg1.attach(msgText1)
    msg.attach(msg1)
    attachment_html = "<html><head><title>%s</title></head><body>%s</body>" \
        "</html>" % (subject, content["html"])
    part = MIMEBase('text', "html")
    part.set_payload( attachment_html )
    part.add_header('Content-Disposition', \
        'attachment; filename="report_%s.html"' % datetime.datetime.now().\
        strftime('%Y_%m_%d'))
    msg.attach(part)
    if content.has_key("csv"):
    	attachment_csv = content["csv"]
    	part = MIMEBase('text', "csv")
    	part.set_payload( attachment_csv )
    	part.add_header('Content-Disposition', \
        	'attachment; filename="report_%s.csv"' % datetime.datetime.now().\
        	strftime('%Y_%m_%d'))
    	msg.attach(part)
    msg = msg.as_string()
    if len(toList[1]) != 0:
        server = smtplib.SMTP( smtpServerHost )
        server.sendmail( fromEmail[1], toList[1], msg )
        server.quit()
    else:
        # The email list isn't valid, so we write it to stderr and hope
        # it reaches somebody who cares.
        print >> sys.stderr, "Problem in sending email to: ",toList

def _toStr(toList):
    """Formats outgoing address list
    Args:
	toList(list of str) - email addresses
    """

    names = [formataddr(i) for i in zip(*toList)]
    return ', '.join(names)


if __name__=="__main__":
	text={}
	title=["Time","Hours","AAAAAAAAAAAAAAA"]
	a=TextUtils(title)
	content={"Time":["aaa","ccc","bbb","Total"],"Hours":[10000,30,300000,"",],"AAAAAAAAAAAAAAA":["","","",10000000000]}
	text["text"]=a.printAsTextTable("text",content)
	text["csv"]=a.printAsTextTable("csv",content)
	text["html"]=a.printAsTextTable("html",content)
	text["html"]="<html><body><h2>%s</h2><table border=1>%s</table></body></html>" % ("aaaaa",a.printAsTextTable("html",content),)
	sendEmail((["Tanya Levshina",],["tlevshin@fnal.gov",]),"balalala",text, ("Gratia Operation","tlevshin@fnal.gov"),"smtp.fnal.gov")
