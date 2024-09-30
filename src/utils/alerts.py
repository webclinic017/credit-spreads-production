import smtplib
from abc import ABC, abstractmethod
from typing import Optional
from dataclasses import dataclass
from termcolor import cprint, colored


class baseAlerts(ABC):
    """enforces the same class methods 
    used for logging module 
    on all alerts services"""
    @abstractmethod
    def info(self, message:str):
        pass
    @abstractmethod
    def error(self, message:str):
        pass
    @abstractmethod
    def warning(self, message:str):
        pass
    
class Alerts(baseAlerts):
    """dispatches messages to all NOTIFICATION services
    
    Currently implemented services include:
    - telegram
    - logger
    """
    def __init__(self, services: Optional[list] = None):
        self.services = services
    
    def info(self, message):
        if self.services is None:
            cprint(message, "green")
        else:
            for services in self.services:
                services.info(message)
    
    def error(self, message):
        if self.services is None:
            cprint(message, "red")
        else:
            for services in self.services:
                services.error(message)
    
    def warning(self, message):
        if self.services is None:
            cprint(message, "yellow")
        else:
            for services in self.services:
                services.warning(message)

# @dataclass
# class EmailMessage:
#     subject: str
#     body: str

# # use pydantic
# @dataclass
# class EmailConfig:
#     email_address: str
#     email_password: str

# class EmailService(ABC):
#     @abstractmethod
#     def __setup(self):
#         ...

# class GmailEmailService(EmailService):
#     def __init__(self, config: EmailConfig):
#         self.config = config
#         self.__setup()
    
#     def __setup(self):
#         self.server = smtplib.SMTP("smtp.gmail.com", 587)
#         self.server.starttls()
#         self.server.login(
#             self.config.email_address, 
#             self.config.email_password
#             )
        
#     def send(self, 
#              message: EmailMessage, 
#              recipient: str):
#         message = f"Subject: {message.subject}\n\n{message.body}"
#         self.server.sendmail(
#             from_addr = self.config.email_address, 
#             to_addrs = recipient, 
#             msg = message)