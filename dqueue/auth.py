import binascii
import jwt
import json
import os
import sys
import time
import requests
import os
import click
import logging

logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger()

def find_hexified_secret(): # hexified in in nginx module too
    for n, m in [
                ("home file", lambda: open(os.path.join(os.environ.get("HOME"), "gateway-secret-hexified"),"r").read().strip()),
                ("env", lambda: os.environ.get("GATEWAY_SECRET_HEXIFIED").strip())
             ]:
        try:
            r = m()
            logger.warning("managed to discover secret with %s", n)
            return r
        except Exception as e:
            logger.warning("failed to discover secret with %s: %s", n, e)
    logger.warning("all secret discovery methods failed: auth will not work")

def decode(token, secret=None, verify=True):
    if verify:
        if secret is None:
            secret = binascii.unhexlify(find_hexified_secret())

        data = jwt.decode(token, key=secret)
    else:
        data = jwt.decode(token, verify=False)

    logger.info("decoded %s", data)

    return data

def generate(output=None, secret=None, lifetime=3*24*3600):
    if secret is None:
        secret = binascii.unhexlify(find_hexified_secret())

    data = {}

    data['lastName']="myself"
    data['subject']="odaapi"
    data['emailAddress']="v@odahub.io"

    data['exp']=int(time.time()+lifetime)

    cjwt=jwt.encode(data, key=secret)

    if output is None:
        f=sys.stdout
    else:
        f=open(output,"wt")

    logger.info("decodes to %s", decode(cjwt))

    f.write(cjwt.decode())

    payload={"action": "new token"}

    r=requests.get(
                "https://dqueue.staging-1-3.odahub.io/",
                data=json.dumps(payload),
                headers={
                        'content-type': 'application/json',
                        'Authorization': 'Bearer ' + cjwt.decode(),
                    }
            )

    print(r.status_code)
    print(r.content)
    print(r.headers)

@click.group("auth")
def auth():
    pass

@auth.command("decode")
@click.argument("token")
@click.option("-s","--secret", default=None, type=str)
def _decode(token, secret=None):
    return decode(token, secret)


@auth.command("generate")
@click.option("-o","--output", default=None)
@click.option("-s","--secret", default=None, type=str)
@click.option("-l","--lifetime", default=3*24*3600, type=float)
def _generate(output=None, secret=None, lifetime=None):
    return generate(output, secret, lifetime)

if __name__ == "__main__":
    auth()
