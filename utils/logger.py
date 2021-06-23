import logging
import time
class Logger:
 
    def __init__(self,JOB_NAME="GLUE_ETL_JOB"):
    # Initialize you log configuration using the base class
        self.job_name = JOB_NAME
        self.logger = self.set_logger()
        self.disable_botocore_debugging()
 
    def set_logger(self):
        """
        ###########################################################################
        ### Logging Configuration
        ###########################################################################
        """
        MSG_FORMAT = f"gluelogs:[{self.job_name}] %(asctime)s %(levelname)s %(name)s: %(message)s"
        DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S %Z'
        logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
        logging.Formatter.converter = time.gmtime
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        return logger
    
    def get_logger(self):
        return self.logger
    
    def disable_botocore_debugging(self):
        """
        When logging Level is Set to DEBUG, DEBUG logs of botocore,urllib3 logs gets appended
        Hence Disabling unwanted logs related to botocore.
        """
        logging.getLogger('botocore').setLevel(logging.WARN)
        # logging.getLogger('urllib3').setLevel(logging.CRITICAL)
    
    def logQuery(self,query):
        self.logger.warning("===================================================")
        self.logger.warning("Executing query:")
        self.logger.warning(query)
        self.logger.warning("===================================================")

    def logJobStart(self):
        self.logger.info("===================================================")
        self.logger.info("Starting Job  : " + self.job_name)
        self.logger.info("===================================================")

    def logJobEnd(self):
        self.logger.warning("===================================================")
        self.logger.warning("Ending Job : " + self.job_name)
        self.logger.warning("===================================================")

if __name__ == "__main__":
    logger = Logger().get_logger()
    Logger().logJobStart()
    Logger().logJobEnd()
    