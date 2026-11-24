import pymongo
import pandas as pd
from datetime import datetime, timedelta, timezone
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
import io
import os
from dotenv import load_dotenv

load_dotenv()

# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------
LOCAL_MONGO_URI = os.getenv("MONGO_URI")
if not LOCAL_MONGO_URI:
    raise Exception("LOCAL_MONGO_URI not found in environment variables") 
LOCAL_DB = "domain_expires"

COLLECTIONS_TO_CHECK = [
    "apexfocusgroup_expires",
  # "newearnhaus_expires",
    "levelsurveys_expires"
]

LIMIT = 2000
DAYS_AGO = 10

# Email Config
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_SENDER = "saidixitn@gmail.com"
SMTP_RECEIVERS = [
    # "harikumar.gadu@jobiak.com",
    "mounika.koviri@jobiak.com",  
    "tarunteja.palla@jobiak.com"   
]
SMTP_APP_PASSWORD = os.getenv("SMTP_PASS")
if not SMTP_APP_PASSWORD:
    raise Exception("SMTP_APP_PASSWORD not found in environment variables") 

# --------------------------------------------------
# MAIN EXECUTION
# --------------------------------------------------
if __name__ == "__main__":
    print("\n========== IN-MEMORY EXPORT & EMAIL SCRIPT STARTING ==========\n")

    # 1. Calculate Target Date (10 days ago from today)
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=DAYS_AGO)
    formatted_cutoff = cutoff_date.strftime('%Y-%m-%d')
    print(f"Target Cutoff Date: {cutoff_date.strftime('%Y-%m-%d %H:%M:%S')} UTC")

    # 2. Connect to Local MongoDB
    try:
        client = pymongo.MongoClient(LOCAL_MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping') # Verify connection
        db = client[LOCAL_DB]
    except Exception as e:
        print(f"Database connection failed: {e}")
        exit(1)

    sheets_data = {}

    # 3. Query Collections
    for coll_name in COLLECTIONS_TO_CHECK:
        print(f"Querying {coll_name}...")

        pipeline = {
            "gpost_date": {
                "$lte": cutoff_date
            },"gpost" : 5
        }

        try:
            cursor = db[coll_name].find(pipeline).limit(LIMIT)
            docs = list(cursor)

            if not docs:
                print(f"  -> No records found older than {DAYS_AGO} days in '{coll_name}'. Skipping sheet.")
                continue

            print(f"  -> Found {len(docs)} records for '{coll_name}'.")

            df = pd.DataFrame(docs)

            if '_id' in df.columns:
                df['_id'] = df['_id'].astype(str)

            for col in df.select_dtypes(include=['datetimetz']).columns:
                df[col] = df[col].dt.tz_localize(None)

            sheet_name = coll_name.replace("_expires", "")
            sheets_data[sheet_name] = df

        except Exception as e:
            print(f"Error processing collection {coll_name}: {e}")

    # 4. Generate Excel File in Memory
    if not sheets_data:
        print("\nNo data found across any collections matching the criteria. Exiting without sending email.")
        exit()

    now = datetime.now()
    date_str = now.strftime('%Y-%m-%d')
    excel_filename = f"DC_Expires_{now.strftime('%b')}_{now.strftime('%d')}.xlsx"

    print(f"\nWriting data to in-memory buffer for {excel_filename}...")

    excel_buffer = io.BytesIO()
    try:
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            for sheet_name, df in sheets_data.items():
                df.to_excel(writer, index=False, sheet_name=sheet_name)
        print("In-memory Excel file generated successfully.")
    except Exception as e:
        print(f"Error generating Excel file: {e}")
        exit(1)

    # 5. Send Email
    print("\nPreparing to send email...")

    # Set up email containers
    msg = MIMEMultipart('mixed')
    msg['From'] = SMTP_SENDER
    # Join list with commas for the header so recipients see each other correctly
    msg['To'] = ", ".join(SMTP_RECEIVERS) 
    msg['Subject'] = f"Automated Expired Jobs Report - {date_str}"

    body_part = MIMEMultipart('alternative')

    # Build Dynamic Client List for HTML
    client_list_html = ""
    client_list_text = ""
    for sheet in sheets_data.keys():
        client_list_html += f'<li style="margin-bottom: 12px;"><strong>{sheet}</strong>: Data extracted and appended to sheet.</li>\n'
        client_list_text += f"- {sheet}\n"

    # Plain Text Fallback
    text_body = (
        f"Hi Team,\n\n"
        f"Please find attached the expired jobs report for {date_str}.\n\n"
        f"Criteria applied:\n"
        f"- gpost_date <= {formatted_cutoff}\n"
        f"- Limit: {LIMIT} records per client\n\n"
        f"Clients included in this attachment:\n"
        f"{client_list_text}\n"
        f"Best regards,\nAutomated System"
    )

    # HTML Body (Cleaned up, button removed, updated wording)
    html_body = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Automated Report</title>
        <style>
            body, table, td, a {{ -webkit-text-size-adjust: 100%; -ms-text-size-adjust: 100%; }}
            table, td {{ mso-table-lspace: 0pt; mso-table-rspace: 0pt; }}
            img {{ -ms-interpolation-mode: bicubic; border: 0; height: auto; line-height: 100%; outline: none; text-decoration: none; }}
            table {{ border-collapse: collapse !important; }}
            body {{ height: 100% !important; margin: 0 !important; padding: 0 !important; width: 100% !important; }}
        </style>
    </head>
    <body style="background-color: #ffffff; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
        <center>
            <table border="0" cellpadding="0" cellspacing="0" width="100%" style="table-layout: fixed; background-color: #ffffff;">
                <tr>
                    <td align="center" style="padding: 40px 15px;">
                        <table border="0" cellpadding="0" cellspacing="0" width="100%" style="max-width: 600px; background-color: #ffffff;">
                            
                            <tr>
                                <td align="center" style="padding-bottom: 40px;">
                                    <span style="font-size: 18px; font-weight: bold; color: #000000; letter-spacing: -0.5px;">Automated Data Systems</span>
                                </td>
                            </tr>

                            <tr>
                                <td align="center" style="padding-bottom: 20px;">
                                    <h1 style="margin: 0; font-size: 32px; font-weight: 500; color: #101010; line-height: 1.2; letter-spacing: -0.5px;">
                                        Daily Expired Jobs<br>Data Report
                                    </h1>
                                </td>
                            </tr>

                            <tr>
                                <td align="center" style="padding: 0 20px 30px 20px;">
                                    <p style="margin: 0 0 15px 0; font-size: 14px; color: #404040; line-height: 1.5; text-align: center;">
                                        Your automated data pipeline has successfully executed. The expired jobs dataset has been aggregated from MongoDB and is <strong>attached to this email</strong>. Please scroll to the attachments section to download your file.
                                    </p>
                                </td>
                            </tr>

                            <tr>
                                <td align="center" style="padding-bottom: 20px;">
                                    <h2 style="margin: 0; font-size: 26px; font-weight: 400; color: #101010; line-height: 1.3; letter-spacing: -0.5px;">
                                        Pipeline Execution Details
                                    </h2>
                                </td>
                            </tr>

                            <tr>
                                <td align="left" style="padding: 0 10px 20px 10px;">
                                    <p style="margin: 0; font-size: 14px; color: #404040; line-height: 1.5;">
                                        The script ran successfully for the scheduled execution on <strong>{date_str}</strong>. Below are the parameters and collections processed:
                                    </p>
                                </td>
                            </tr>

                            <tr>
                                <td align="left" style="padding: 0 10px 20px 10px;">
                                    <ul style="margin: 0; padding-left: 20px; font-size: 14px; color: #404040; line-height: 1.5;">
                                        <li style="margin-bottom: 12px;">
                                            <strong>Target Cutoff Date:</strong> gpost_date &lt;= {formatted_cutoff}
                                        </li>
                                        <li style="margin-bottom: 12px;">
                                            <strong>Query Limit:</strong> Capped at {LIMIT} records per collection.
                                        </li>
                                        {client_list_html}
                                    </ul>
                                </td>
                            </tr>

                            <tr>
                                <td align="left" style="padding: 0 10px 30px 10px;">
                                    <p style="margin: 0; font-size: 14px; color: #404040; line-height: 1.5;">
                                        If you notice any discrepancies in the data, please check the MongoDB source collections or adjust the environment variables in the server configuration.
                                    </p>
                                </td>
                            </tr>

                            <tr>
                                <td style="background-color: #f4f4f4; border-radius: 8px; padding: 30px 20px;">
                                    <table border="0" cellpadding="0" cellspacing="0" width="100%">
                                        <tr>
                                            <td align="left" valign="top" style="font-size: 10px; color: #666666; line-height: 1.6;">
                                                <div style="font-size: 14px; font-weight: bold; color: #000000; margin-bottom: 12px;">Jobiak Reports</div>
                                                Automated Systems Division<br>
                                                Proxy Jobs Export API<br><br>
                                                This is an automated system message.<br>
                                                Please do not reply directly to this email.
                                            </td>
                                        </tr>
                                    </table>
                                </td>
                            </tr>
                            
                        </table>
                    </td>
                </tr>
            </table>
        </center>
    </body>
    </html>
    """

    body_part.attach(MIMEText(text_body, 'plain'))
    body_part.attach(MIMEText(html_body, 'html'))
    msg.attach(body_part)

    # Attach the in-memory Excel file as a standard attachment
    try:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(excel_buffer.getvalue())
        encoders.encode_base64(part)
        
        # Standard header for email attachments
        part.add_header('Content-Disposition', f"attachment; filename={excel_filename}")
        msg.attach(part)

        # SMTP Setup and dispatch
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SMTP_SENDER, SMTP_APP_PASSWORD)

        # Send directly using the SMTP_RECEIVERS list
        server.sendmail(SMTP_SENDER, SMTP_RECEIVERS, msg.as_string())
        server.quit()

        print(f"Email successfully sent to: {', '.join(SMTP_RECEIVERS)}")

    except Exception as e:
        print(f"Error sending email: {e}")
    finally:
        excel_buffer.close()

    print("\n========== SCRIPT COMPLETE ==========\n")