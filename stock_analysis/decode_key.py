import base64
import json
import os
import sys


def main(encode_key):
    base64_decode = base64.b64decode(encode_key)
    json_decode = json.loads(base64_decode.decode())
    os.makedirs("./key", exist_ok=True)
    key_path = os.path.join("./key", "service_account.json")
    with open(key_path, "w") as f:
        json.dump(json_decode, f, indent=4)
    os.chmod(key_path, 0o600)


if __name__ == "__main__":
    # Read the encoded key from stdin so it never appears in shell history or process lists.
    main(sys.stdin.read().strip())
