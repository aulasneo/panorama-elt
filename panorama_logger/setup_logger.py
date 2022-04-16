import logging
import sys

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s  %(name)s  %(levelname)s: '
                              '[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s')
streamHandler = logging.StreamHandler(sys.stdout)
log.addHandler(streamHandler)
streamHandler.setFormatter(formatter)

