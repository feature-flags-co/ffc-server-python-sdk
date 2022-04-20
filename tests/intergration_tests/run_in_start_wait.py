import logging
from ffcclient import get, set_config

from ffcclient.common_types import FFCUser
from ffcclient.config import Config
from ffcclient.utils import TRACE_LEVEL

logging.basicConfig(level=TRACE_LEVEL,
                    format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%m-%d %H:%M')

env_secret = "YjA1LTNiZDUtNCUyMDIxMDkwNDIyMTMxNV9fMzhfXzQ4X18xMDNfX2RlZmF1bHRfNzc1Yjg="

config = Config(env_secret, base_uri='https://api-dev.minjiekaiguan.com', streaming_uri='wss://api-dev.minjiekaiguan.com')

set_config(config)

client = get()

while client.initialize:
    line = input('input user key and flag key seperated by / \n')
    if 'exit' == line.strip():
        break
    try:
        user_key, flag_key, *_ = tuple(line.split('/'))
        user = {'key': user_key, 'name': user_key}
        ffc_user = FFCUser.from_dict(user)
        print(ffc_user.to_json_str())
        print(client.variation_detail(flag_key, user).to_json_str())
    except Exception as e:
        print(e)
        break

client.stop()
