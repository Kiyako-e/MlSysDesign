import logging
import sys

# log_file_name = f'logs.log'
logging.basicConfig(
                    # filename=log_file_name,
                    # filemode='a',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)
logging.getLogger().setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)
