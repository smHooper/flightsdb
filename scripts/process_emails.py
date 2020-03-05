
import sys, os
import imaplib
import email
import smtplib
import pandas as pd
from getpass import getpass
from email.mime import text as mimetext, multipart as mimemultipart, base as mimebase

import read_landing_submission as submission

CONTACT_FOR_SCENIC = 'samuel_hooper@nps.gov' # this should come from some config file
CONTACT_FOR_TRACKS = 'samuel_hooper@nps.gov'

def download_attachments(email_message, download_dir, overwrite=True):
    download_info = pd.DataFrame(columns=['filename', 'failure_reason', 'message_body'])

    if email_message.is_multipart():
        for part in email_message.walk():
            filename = 'none' # reset these vars for each iteration
            failure_reason = ''
            body = ''
            content_type = part.get_content_type()
            content_disposition = str(part.get('Content-Disposition'))

            if content_type == 'text/plain' and 'attachment' not in content_disposition:
                body = part.get_payload(decode=True).decode('utf-8')
            elif 'attachment' in content_disposition:
                filename = part.get_filename()
                download_path = os.path.join(download_dir, filename)
                if not os.path.isfile(download_path) or overwrite:
                    try:
                        with open(download_path, 'wb') as f:
                            f.write(part.get_payload(decode=True))
                    except Exception as e:
                        failure_reason = 'download failed because %s' % e
                else:
                    failure_reason = 'file already exists'
            else:
                continue

            download_info = download_info.append(
                pd.DataFrame([[filename, failure_reason, body]],
                             columns=['filename', 'failure_reason', 'message_body']
                             ),
                ignore_index=True
            )

    else:
        body = email_message.get_payload(decode=True).decode('utf-8')
        download_info = download_info.append(
            pd.DataFrame([{'filename': 'none', 'failure_reason': 'no attachments found', 'message_body': body}]),
            ignore_index=True
        )

    if (download_info.filename.str.len() == 0).all():
        download_info.failure_reason = 'no attachments found'

    return download_info


def read_unread_emails(username, password, download_dir, credentials_txt=None):

    client = imaplib.IMAP4_SSL('imap.gmail.com')
    client.login(username, password)
    client.select('inbox')

    result, uid_bytes = client.uid('search', None, 'UNSEEN')  # search and return uids instead
    uid_strs = uid_bytes[0].split()

    messages = pd.DataFrame(columns=['uid', 'from_address', 'subject', 'message_body', 'filename', 'failure_reason'])
    for uid in uid_strs:
        result, data = client.uid('fetch', uid, '(RFC822)')
        email_bytes = data[0][1]

        email_message = email.message_from_bytes(email_bytes)
        #from_name, from_address = email.utils.parseaddr(email_message['From'])

        download_info = download_attachments(email_message, download_dir)
        download_info['uid'] = uid
        download_info['from_address'] = email_message['From']#from_address
        download_info['subject'] = email_message['Subject']
        messages = messages.append(download_info, ignore_index=True, sort=False)

    client.close()

    return messages


def get_email_credentials(credentials_txt):

    username = None
    password = None
    with open(credentials_txt) as txt:
        for line in txt.readlines():
            if line.startswith('username'):
                username = line.split(':')[1].strip()
            elif line.startswith('password'):
                password = line.split(':')[1].strip()

    if not username:
        username = getpass('No username found in text file. Username?: ')
    if not password:
        password = getpass('No password found in text file. Password}?: ')

    return username, password


def send_email(message_body, subject, sender, recipients, server, attachments=[], message_body_type='plain'):

    msg_root = mimemultipart.MIMEMultipart('mixed')
    msg_root['Date'] = email.utils.formatdate(localtime=True)
    msg_root['From'] = sender
    msg_root['To'] = ', '.join(recipients)
    msg_root['Subject'] = email.header.Header(subject, 'utf-8')

    msg_txt = mimetext.MIMEText(message_body, message_body_type, 'utf-8')
    msg_root.attach(msg_txt)

    for attachment in attachments:
        filename = os.path.basename(attachment)

        with open(attachment, 'rb') as f:
            msg_attach = mimebase.MIMEBase('application', 'octet-stream')
            msg_attach.set_payload(f.read())
            email.encoders.encode_base64(msg_attach)
            msg_attach.add_header('Content-Disposition', 'attachment',
                                  filename=(email.header.Header(filename, 'utf-8').encode()))
            msg_root.attach(msg_attach)

    server.send_message(msg_root)


def main(download_dir, credentials_txt, connection_txt):

    if credentials_txt:
        username, password = get_email_credentials(credentials_txt)
    else:
        username = input('Username: ')
        password = input('Password: ')

    file_status = read_unread_emails(username, password, download_dir, credentials_txt)

    for i, path in file_status.loc[file_status.filename != 'none', 'filename'].iteritems():
        # If it's data, import it
        if path.endswith('.xlsx') or path.endswith('.xls'):
            try:
                submission.import_data(path, connection_txt)
            except Exception as e:
                file_status.loc[i, 'failure_reason'] = e
        else:
            file_status.loc[i, 'failure_reason'] = 'unrecognized file extension'

    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.ehlo()
    server.login(username, password)

    # For each new email found, send a reply with a summary of the files processed
    for _, message in file_status.groupby('uid'):
        message_info = message.iloc[0].copy() # get sender, subject, body, etc.
        reply_txt = 'Thank you for your flight data submission. '

        # If there's a filename, and there was no failure_reason, mark it as a success
        message['success'] = ~message.failure_reason.str.len().astype(bool) & (message.filename != 'none')
        fwd_txt = '' # init in case there were no problems with any attachments

        # No files were submitted. Assuming this was an error, send a reply notifying the sender
        if (message.failure_reason == 'no attachment found').all():
            reply_txt += 'It looks like, however, that you did not attach any files. If this was ' \
                         'unintentional, please resend your email with the intended files attached. ' \
                         'Otherwise, note that this email address is not monitored by a human; please ' \
                         'address any questions or concerns to the flight data manager at %s.' % \
                         CONTACT_FOR_SCENIC

        # All files were successfully processed
        elif message.success.all():
            reply_txt += 'All of the files you submitted were successfully processed.'

        # At least some files failed
        else:
            failed_files = message.loc[~message.success]
            failed_file_str = '\n\t\u2022 '.join(failed_files.filename + ': ' + failed_files.failure_reason)
            reply_txt += 'Unfortunately, processing failed for the following files:' \
                       '\n\t\u2022 ' + failed_file_str + \
                       '. \n\nThe flight data management team will attempt to correct the errors and process ' \
                       'the files. If they are unable to do so, you will be notified. If you have ' \
                       'any questions or concerns, please contact the flight data manager at %s.' % CONTACT_FOR_SCENIC

            fwd_txt = ('There was at least one problem with a recent flight data submission from {submitter}. See '
                       'below for descriptions of the errors. Please try to correct these errors if possible, and '
                       'contact the submitter if you need more information. Note that any files that failed to '
                       'download are not attached to this email. You will have to retrieve them from the original '
                       'message sent to {submission_address}\n\nErrors:\n{errors}\n\nOriginal message:\n{body}')\
                .format(submitter=email.utils.parseaddr(message_info.from_address)[1],
                        submission_address=username,
                        errors=failed_file_str,
                        body=message_info.message_body)
            # Forward the email with attachments to
            files_to_attach = failed_files.loc[failed_files.filename != 'none']
            send_email(fwd_txt, 'FWD: %s' % message_info.subject, username, [CONTACT_FOR_SCENIC], server,
                       attachments=list(map(lambda x: os.path.join(download_dir, x), files_to_attach.filename)))

        reply_txt += '\n\n\n***NOTE: Do not reply to this message. This email address is not monitored ' \
                     'by a human. Please address any questions or concerns to %s***\n' % CONTACT_FOR_SCENIC

        send_email(reply_txt, 'RE: %s' % message_info.subject, username, [message_info.from_address], server)

    server.quit()


if __name__ == '__main__':
    sys.exit(main(*sys.argv[1:]))
