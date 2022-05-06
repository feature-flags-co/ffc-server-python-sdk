import logging
from ffcclient import get, set_config

from ffcclient.common_types import FFCUser
from ffcclient.config import Config
from ffcclient.utils import log

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%m-%d %H:%M')

env_secret = "ZDMzLTY3NDEtNCUyMDIxMTAxNzIxNTYyNV9fMzZfXzQ2X185OF9fZGVmYXVsdF80ODEwNA=="

config = Config(env_secret,
                base_uri='https://api-dev.featureflag.co',
                streaming_uri='wss://api-dev.featureflag.co')

set_config(config)

client = get()

all_flag_values = None
last_user_key = ''

while client.initialize:
    line = input('input user key and flag key seperated by / \n')
    if 'exit' == line.strip():
        break
    try:
        user_key, flag_key, *_ = tuple(line.split('/'))
        user = {'key': user_key, 'name': user_key}
        ffc_user = FFCUser.from_dict(user)
        log.info('FFC Python SDK Test: user= %s' % ffc_user.to_json_str())
        if(last_user_key != user_key or not all_flag_values):
            all_flag_values = client.get_all_latest_flag_variations(user)
        ed = all_flag_values.get(flag_key)
        last_user_key = user_key
        log.info('FFC Python SDK Test: variation= %s' % ed.to_json_str())
    except:
        log.exception('FFC Python SDK Test: unexpected error')
        break

client.stop()
