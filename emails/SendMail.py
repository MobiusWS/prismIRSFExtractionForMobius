import os.path
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders


def sendEmail(mailContent, toEmailAddress, attachments):
    mail_content = mailContent
    #The mail addresses and password
    sender_address = 'hnagar@gmail.com'
    sender_pass = 'vtswtyewapcnaxzv '
    receiver_address = 'hnagar@gmail.com'
    mailSubject = "Latest IRSF list from Mobius"
    #Setup the MIME
    message = MIMEMultipart()
    message['From'] = sender_address
    message["Subject"] = mailSubject

    for i in range(len(toEmailAddress)):
        message['To'] = toEmailAddress[i]['email']
        #The subject line
        #The body and the attachments for the mail
        for attachedFile in attachments:
            filename = os.path.basename(attachedFile)
            message.attach(MIMEText(mail_content, 'plain'))
            attach_file_name = attachedFile
            attach_file = open(attach_file_name, 'rb') # Open the file as binary mode
            payload = MIMEBase('application', 'octet-stream')
            payload.set_payload(attach_file.read())
            encoders.encode_base64(payload)

            #encode the attachment
            #add payload header with filename
            payload.add_header('Content-Disposition', 'attachment; filename="%s"'%filename)
            message.attach(payload)
        #Create SMTP session for sending the mail
        print(i)
        print(toEmailAddress[i]['email'])
        session = smtplib.SMTP('smtp.gmail.com', 587) #use gmail with port
        session.starttls() #enable security
        session.login(sender_address, sender_pass) #login with mail_id and password
        text = message.as_string()
        session.sendmail(sender_address, toEmailAddress[i]['email'], text)