"""Shared logger configuration for the Panorama ELT package."""
import logging
import sys

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s  %(name)s  %(levelname)s: '
                              '[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s')
stream_handler = logging.StreamHandler(sys.stdout)
log.addHandler(stream_handler)
stream_handler.setFormatter(formatter)
