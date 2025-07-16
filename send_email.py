import smtplib
from email.message import EmailMessage

EMAIL_CONFIG = {
    "smtp_server": "ssl0.ovh.net",  # Serveur SMTP OVH
    "smtp_port": 587,               # Port STARTTLS
    "username": "support@locacoeur.com",  # Adresse complète obligatoire
    "password": "86Hqw6O&8i*i",     # Mot de passe de la boîte mail OVH
    "from_email": "support@locacoeur.com",
    "to_emails": ["berradaadam63@gmail.com"],
    "enabled": True  # Mets à True pour activer l'envoi
}

def send_email(subject: str, body: str):
    if not EMAIL_CONFIG["enabled"]:
        print("Email sending is disabled in configuration.")
        return

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = EMAIL_CONFIG["from_email"]
    msg["To"] = ", ".join(EMAIL_CONFIG["to_emails"])
    msg.set_content(body)

    try:
        with smtplib.SMTP(EMAIL_CONFIG["smtp_server"], EMAIL_CONFIG["smtp_port"]) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
            smtp.login(EMAIL_CONFIG["username"], EMAIL_CONFIG["password"])
            smtp.send_message(msg)
        print("Email sent successfully.")
    except Exception as e:
        print(f"Failed to send email: {e}")

# Exemple d'envoi
if __name__ == "__main__":
    send_email("Test Email OVH", "CC ADAM.")
