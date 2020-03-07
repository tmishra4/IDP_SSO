import smtplib


def _send_email(sender, receivers, message):
    smtpObj = smtplib.SMTP('mailhost.auckland.ac.nz')
    smtpObj.sendmail(sender, receivers, message)

