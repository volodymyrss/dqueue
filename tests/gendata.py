import ast
import json
import base64
import click
import astropy.io.fits as fits


@click.group()
def cli():
    pass

@cli.command()
def gen():
    data = {
        'ima': base64.b64encode(open("/mnt/sshfs/cdcihn/unsaved/astro/savchenk/data/reduced/ddcache/byscw/2220/222000690010.000/ii_skyimage.v2/7574b1dc/isgri_sky_ima.fits.gz", "rb").read()).decode()
    }

    json.dump(data, open("data.json", "wt"))

    dag = ast.literal_eval(open("/mnt/sshfs/cdcihn/unsaved/astro/savchenk/data/reduced/ddcache/byscw/2220/222000690010.000/ii_skyimage.v2/7574b1dc/hash.txt", "rt").read())

    json.dump(dag, open("dag.json", "wt"))

@cli.command()
@click.argument("fn")
def load(fn):
    p = json.load(open(fn, "rt"))

    open("ima.fits", "wb").write(base64.b64decode(json.loads(p['data_json'])['ima']))

    print(fits.open("ima.fits"))
    


if __name__ == "__main__":
    cli()
