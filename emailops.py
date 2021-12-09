import smtplib
from tabulate import tabulate
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders
from email.mime.base import MIMEBase
import platform


class EmailOps(object):
    """
    Form an email configuration object for other operation
    """
    def __init__(self):
        """
        Construct an email configuration object
        """
        self._heading = "<p></p>"
        self._ending = "<p></p>"

        self._email_Configuration = {
            "Subject": None,
            "From": None,
            "To": None,
            "SMTP": None,
            "Cc": None,
            "Bcc": None,
            "Password": None,
            "Attachment": None,
            "Body": self._update_html()
        }

    def _update_html(self):
        self.html = f"""
        <html>
            <head>
                <style> 
                  table, th, td {{{{ border: 1px solid black; border-collapse: collapse; }}}}
                  th, td {{{{ padding: 5px; }}}}
                </style>
            </head>
            <body>
                {self._heading}

                {{table}}

                {self._ending}
            </body>
        </html>
        """
        return self.html

    def add_heading(self, content: str):
        """
        Add new heading to the email body
        :param content: Content of the heading
        :return: None
        """
        self._heading += f'<p>{content}</p>'
        self._email_Configuration["Body"] = self._update_html()

    def add_ending(self, content: str):
        """
        Add new ending to the email body
        :param content: Content of the email ending
        :return: None
        """
        self._ending += f'<p>{content}</p>'
        self._email_Configuration["Body"] = self._update_html()

    def __setitem__(self, key: str, value: str):
        """
        Set a value for the email configuration file
        :param key: Key name
        :param value: Key's value
        :return: None
        """
        if key not in self._email_Configuration:
            raise KeyError(f'Key \"{key}\" does not exist')
        self._email_Configuration[key] = value

    def __getitem__(self, key: str):
        """
        Get a value by key name
        :param key: Key name
        :return: The value of the key
        """
        if key not in self._email_Configuration:
            raise KeyError(f'Key \"{key}\" does not exist')
        return self._email_Configuration[key]

    def __str__(self):
        """
        Print string
        :return: String format of the configuration
        """
        return str(self._email_Configuration)

    def send_email(self, data: list):
        """
        Send an email with required information
        :param data: The data in a list of lists format, which will form a HTML table in the final email.
        :return: N/A
        """
        required_key_list = ["Subject", "From", "To", "SMTP", "Password", "Body"]

        for key in required_key_list:
            if self._email_Configuration[key] is None:
                raise ValueError(f'Key \"{key}\" is None.')

        html = self._email_Configuration["Body"].format(table=tabulate(data, headers="firstrow", tablefmt="html"))

        message = MIMEMultipart("alternative", None, [MIMEText(html, "html")])
        message["Subject"] = self._email_Configuration["Subject"]
        message["From"] = self._email_Configuration["From"]
        message["To"] = self._email_Configuration["To"]

        if self._email_Configuration["Cc"] is not None:
            message["Cc"] = self._email_Configuration["Cc"]

        if self._email_Configuration["Bcc"] is not None:
            message["Bcc"] = self._email_Configuration["Bcc"]

        if self._email_Configuration["Attachment"] is not None:
            for file in self._email_Configuration["Attachment"]:
                try:
                    part = MIMEBase('application', "octet-stream")
                    part.set_payload(open(file, "rb").read())
                    encoders.encode_base64(part)
                    if platform.system() == "Windows":
                        file_name = file
                    else:
                        file_name = file.split("/")[2]
                    part.add_header('Content-Disposition', 'attachment', filename=file_name)
                    message.attach(part)
                except Exception as e:
                    raise e
                finally:
                    continue

        server = smtplib.SMTP(self._email_Configuration["SMTP"])
        server.ehlo()
        server.starttls()
        if self._email_Configuration["Password"] is not None:
            server.login(self._email_Configuration["From"], self._email_Configuration["Password"])
        server.sendmail(self._email_Configuration["From"], [self._email_Configuration["To"]][0].split(","), message.as_string())
        server.quit()
