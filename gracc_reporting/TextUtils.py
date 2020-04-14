"""This module provides static methods to create ascii, csv, and html attachment and send email to specified group of people. """

import time
import sys
import datetime
from io import StringIO
import smtplib
from email.message import EmailMessage
import tabulate
import pandas as pd
from email.utils import formataddr

from . import NiceNum


##########################################
# This code is partially taken from      #
# AccountingReports.py                   #
##########################################
class TextUtils:
    """Formats the text to create ascii, csv, and html attachment  and send email to specified group of people. """

    def __init__(self, table_header):
        """Args:
            table_header(list of str) - the header row for the output table
        """
        self.table_header = table_header

    def getWidth(self, l):
        """Returns max length of string in the list - needed for text formating of the table
            l(list of str)
        """

        return max(len(repr(s)) for s in l)

    def getLength(self, text):
        """Returns number of rows in the table
        Args:
            text(list)
        """

        return len(text[self.table_header[0]])

    def printAsTextTable(self, format_type, text, template=False):
        """"Prepares input text to send as attachment
        Args:
            format_type(str) - text, csv, html
            text (dict of lists) - {column_name:[values],column_name:[values]} where column_name corresponds to header name
        """

        # Convert list of dicts to pandas data frame
        df = pd.DataFrame.from_dict(text, orient='index').transpose()
        # Order the columns according to the header
        df = df[self.table_header]

        # the order is defined by header list
        if format_type == "text":
            return tabulate.tabulate(df, tablefmt="grid", headers=self.table_header, showindex=False, floatfmt=',.1f')
        elif format_type == "html":
            return tabulate.tabulate(df, tablefmt="html", headers=self.table_header, showindex=False, floatfmt=',.1f')
        elif format_type == "csv":
            return df.to_csv(index=False)


def sendEmail(toList, subject, content, fromEmail=None, smtpServerHost=None, html_template=False):
    """
    This turns the "report" into an email attachment and sends it to the EmailTarget(s).
    Args:
    toList(list of str) - list of emails addresses
    content(str) - email content
    fromEmail (str) - from email address
    smtpServerHost(str) - smtpHost
    """

    #Charset.add_charset('utf-8', Charset.QP, Charset.QP, 'utf-8')

    if toList[1] is None:
        print("Cannot send mail (no To: specified)!", file=sys.stderr)
        sys.exit(1)

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = formataddr(fromEmail)
    msg["To"] = _toStr(toList)
    # new code
    if "text" in content:
        msg.set_content(content["text"], 'plain')
        msg.add_alternative("<pre>" + content["text"] + "</pre>", subtype="html")

    if html_template:
        attachment_html = content["html"]
    else:
        attachment_html = "<html><head><title>%s</title></head><body>%s</body>" \
                      "</html>" % (subject, content["html"])

    msg.add_attachment(attachment_html, filename="report_{}.html".format(datetime.datetime.now().strftime('%Y_%m_%d')))
    if "csv" in content:
        msg.add_attachment(content["csv"], filename="report_{}.csv".format(datetime.datetime.now().strftime('%Y_%m_%d')))

    msg = msg.as_string()

    if len(toList[1]) != 0:
        server = smtplib.SMTP(smtpServerHost)
        server.sendmail(fromEmail[1], toList[1], msg)
        server.quit()
    else:
        # The email list isn't valid, so we write it to stderr and hope
        # it reaches somebody who cares.
        print("Problem in sending email to: ", toList, file=sys.stderr)


def _toStr(toList):
    """Formats outgoing address list
    Args:
    toList(list of str) - email addresses
    """

    names = [formataddr(i) for i in zip(*toList)]
    return ', '.join(names)


if __name__ == "__main__":
    text = {}
    title = ["Time", "Hours", "AAAAAAAAAAAAAAA"]
    a = TextUtils(title)
    content = {"Time": ["aaa", "ccc", "bbb", "Total"],
               "Hours": [10000, 30, 300000, "", ],
               "AAAAAAAAAAAAAAA": ["", "", "", 10000000000]}
    text["text"] = a.printAsTextTable("text", content)
    text["csv"] = a.printAsTextTable("csv", content)
    text["html"] = a.printAsTextTable("html", content)
    text[
        "html"] = "<html><body><h2>%s</h2><table border=1>%s</table></body></html>" % (
    "aaaaa", a.printAsTextTable("html", content),)
    sendEmail((["Tanya Levshina", ], ["tlevshin@fnal.gov", ]), "balalala",
              text, ("Gratia Operation", "tlevshin@fnal.gov"), "smtp.fnal.gov")
