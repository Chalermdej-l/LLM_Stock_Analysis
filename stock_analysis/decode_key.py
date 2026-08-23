import base64
import json
import os
import argparse
def main(encode_key):
    base64_decode = base64.b64decode(encode_key)
    json_decode = json.loads(base64_decode.decode())
    os.makedirs('./key',exist_ok=True)
    with open('./key/service_account.json','w+')as f:
        json.dump(json_decode,f,indent=4)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--encode_key',type=str,required=True)
    args = parser.parse_args()
    main(args.encode_key)