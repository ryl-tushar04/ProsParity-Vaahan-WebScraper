import smtplib
import os
import ssl
from email.message import EmailMessage


def send_csv_via_email(recipient_email, attachment_path):
    """
    Sends the processed CSV file to the specified recipient using Gmail.
    Requires SENDER_EMAIL and SENDER_PASSWORD (App Password) in environment variables.
    """
    # 1. Load Credentials (Best practice: Load from environment variables)
    sender_email = os.environ.get("SENDER_EMAIL")
    sender_password = os.environ.get("SENDER_PASSWORD")

    if not sender_email or not sender_password:
        return False, "❌ Missing credentials. Please set SENDER_EMAIL and SENDER_PASSWORD in environment."

    if not os.path.exists(attachment_path):
        return False, f"❌ File not found: {attachment_path}"

    # 2. Create the Email Message
    msg = EmailMessage()
    msg['Subject'] = 'Your Vahan Data Automation is Complete'
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg.set_content(
        "Hello,\n\nThe Vahan automation pipeline has finished processing. Please find the merged CSV file attached.\n\nBest,\nYour Automation Pipeline")

    # 3. Attach the File
    try:
        with open(attachment_path, 'rb') as f:
            file_data = f.read()
            file_name = os.path.basename(attachment_path)

        msg.add_attachment(file_data, maintype='application', subtype='octet-stream', filename=file_name)
    except Exception as e:
        return False, f"❌ Error attaching file: {str(e)}"

    # 4. Send via SMTP
    context = ssl.create_default_context()
    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465,context=context) as smtp:
            smtp.login(sender_email, sender_password)
            smtp.send_message(msg)
        return True, f"✅ Email successfully sent to {recipient_email}"
    except Exception as e:
        return False, f"❌ Failed to send email: {str(e)}"