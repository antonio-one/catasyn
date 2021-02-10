import logging
from catasyn.settings import SCHEDULER_LOG_FILENAME

FORMAT = "%(asctime)s %(levelname)s %(message)s"
logging.basicConfig(filename=SCHEDULER_LOG_FILENAME, level=logging.DEBUG, format=FORMAT)